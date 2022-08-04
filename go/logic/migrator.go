/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"
)

type ChangelogState string

const (
	GhostTableMigrated         ChangelogState = "GhostTableMigrated"
	AllEventsUpToLockProcessed                = "AllEventsUpToLockProcessed"
)

func ReadChangelogState(s string) ChangelogState {
	return ChangelogState(strings.Split(s, ":")[0])
}

type tableWriteFunc func() error

type applyEventStruct struct {
	writeFunc *tableWriteFunc
	dmlEvent  *binlog.BinlogDMLEvent
}

// writeFunc赋值
func newApplyEventStructByFunc(writeFunc *tableWriteFunc) *applyEventStruct {
	result := &applyEventStruct{writeFunc: writeFunc}
	return result
}

// dmlEvent赋值
func newApplyEventStructByDML(dmlEvent *binlog.BinlogDMLEvent) *applyEventStruct {
	result := &applyEventStruct{dmlEvent: dmlEvent}
	return result
}

type PrintStatusRule int

const (
	NoPrintStatusRule           PrintStatusRule = iota
	HeuristicPrintStatusRule                    = iota
	ForcePrintStatusRule                        = iota
	ForcePrintStatusOnlyRule                    = iota
	ForcePrintStatusAndHintRule                 = iota
)

// Migrator is the main schema migration flow manager.
type Migrator struct {
	parser           *sql.AlterTableParser
	inspector        *Inspector
	applier          *Applier
	eventsStreamer   *EventsStreamer
	server           *Server
	throttler        *Throttler
	hooksExecutor    *HooksExecutor
	migrationContext *base.MigrationContext

	firstThrottlingCollected   chan bool
	ghostTableMigrated         chan bool
	rowCopyComplete            chan error
	allEventsUpToLockProcessed chan string

	rowCopyCompleteFlag int64
	// copyRowsQueue should not be buffered; if buffered some non-damaging but
	//  excessive work happens at the end of the iteration as new copy-jobs arrive before realizing the copy is complete
	copyRowsQueue    chan tableWriteFunc
	applyEventsQueue chan *applyEventStruct

	handledChangelogStates map[string]bool

	finishedMigrating int64
}

func NewMigrator(context *base.MigrationContext) *Migrator {
	migrator := &Migrator{
		migrationContext:           context,
		parser:                     sql.NewAlterTableParser(),
		ghostTableMigrated:         make(chan bool),
		firstThrottlingCollected:   make(chan bool, 3),
		rowCopyComplete:            make(chan error),
		allEventsUpToLockProcessed: make(chan string),

		copyRowsQueue:          make(chan tableWriteFunc),
		// applyEventsQueue 事件队列，最大长度由MaxEventsBatchSize决定
		applyEventsQueue:       make(chan *applyEventStruct, base.MaxEventsBatchSize),
		handledChangelogStates: make(map[string]bool),
		finishedMigrating:      0,
	}
	return migrator
}

// initiateHooksExecutor
func (this *Migrator) initiateHooksExecutor() (err error) {
	this.hooksExecutor = NewHooksExecutor(this.migrationContext)
	if err := this.hooksExecutor.initHooks(); err != nil {
		return err
	}
	return nil
}

// sleepWhileTrue sleeps indefinitely until the given function returns 'false'
// (or fails with error)
func (this *Migrator) sleepWhileTrue(operation func() (bool, error)) error {
	for {
		shouldSleep, err := operation()
		if err != nil {
			return err
		}
		if !shouldSleep {
			return nil
		}
		time.Sleep(time.Second)
	}
}

// retryOperation attempts up to `count` attempts at running given function,
// exiting as soon as it returns with non-error.
func (this *Migrator) retryOperation(operation func() error, notFatalHint ...bool) (err error) {
	// MaxRetries 获取最大重试次数
	maxRetries := int(this.migrationContext.MaxRetries())
	for i := 0; i < maxRetries; i++ {
		if i != 0 {
			// sleep after previous iteration
			time.Sleep(1 * time.Second)
		}
		err = operation()
		if err == nil {
			return nil
		}
		// there's an error. Let's try again.
	}
	// 如果是FatalHint，重试后依然失败则调用PanicAbort
	if len(notFatalHint) == 0 {
		this.migrationContext.PanicAbort <- err
	}
	return err
}

// `retryOperationWithExponentialBackoff` attempts running given function, waiting 2^(n-1)
// seconds between each attempt, where `n` is the running number of attempts. Exits
// as soon as the function returns with non-error, or as soon as `MaxRetries`
// attempts are reached. Wait intervals between attempts obey a maximum of
// `ExponentialBackoffMaxInterval`.
func (this *Migrator) retryOperationWithExponentialBackoff(operation func() error, notFatalHint ...bool) (err error) {
	var interval int64
	maxRetries := int(this.migrationContext.MaxRetries())
	maxInterval := this.migrationContext.ExponentialBackoffMaxInterval
	for i := 0; i < maxRetries; i++ {
		newInterval := int64(math.Exp2(float64(i - 1)))
		if newInterval <= maxInterval {
			interval = newInterval
		}
		if i != 0 {
			time.Sleep(time.Duration(interval) * time.Second)
		}
		err = operation()
		if err == nil {
			return nil
		}
	}
	if len(notFatalHint) == 0 {
		this.migrationContext.PanicAbort <- err
	}
	return err
}

// executeAndThrottleOnError executes a given function. If it errors, it
// throttles.
func (this *Migrator) executeAndThrottleOnError(operation func() error) (err error) {
	if err := operation(); err != nil {
		this.throttler.throttle(nil)
		return err
	}
	return nil
}

// consumeRowCopyComplete blocks on the rowCopyComplete channel once, and then
// consumes and drops any further incoming events that may be left hanging.
func (this *Migrator) consumeRowCopyComplete() {
	// 主进程阻塞，直到接收到rowCopyComplete 信道的消息，接收到则认为rowCopy完成
	if err := <-this.rowCopyComplete; err != nil {
		this.migrationContext.PanicAbort <- err
	}
	atomic.StoreInt64(&this.rowCopyCompleteFlag, 1)
	this.migrationContext.MarkRowCopyEndTime()
	// 开启一个协程，rowCopy完成之后继续接收，如果接收到err则退出
	go func() {
		for err := range this.rowCopyComplete {
			if err != nil {
				this.migrationContext.PanicAbort <- err
			}
		}
	}()
}

// 根据context的CutOverCompleteFlag来判断是否可以停止binlog stream，即CutOver之前不能停止streaming
func (this *Migrator) canStopStreaming() bool {
	return atomic.LoadInt64(&this.migrationContext.CutOverCompleteFlag) != 0
}

// onChangelogEvent is called when a binlog event operation on the changelog table is intercepted.
// onChangelogEvent 根据心跳表中hint关键字返回对应的处理函数
func (this *Migrator) onChangelogEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	// Hey, I created the changelog table, I know the type of columns it has!
	// 根据binlog event中对应心跳表的第三列(hint列)的ASCII字符串；判断是否为“state”或者“heartbeat”关键字，返回对应处理函数
	switch hint := dmlEvent.NewColumnValues.StringColumn(2); hint {
	case "state":
		return this.onChangelogStateEvent(dmlEvent)
	case "heartbeat":
		return this.onChangelogHeartbeatEvent(dmlEvent)
	default:
		return nil
	}
}

// 处理心跳表中的事件(GhostTableMigrated/AllEventsUpToLockProcessed)
func (this *Migrator) onChangelogStateEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	// 拦截心跳表状态，处理对应事件hint，根据binlog event中对应心跳表的第四列(value列)的ASCII字符串
	changelogStateString := dmlEvent.NewColumnValues.StringColumn(3)
	changelogState := ReadChangelogState(changelogStateString)
	this.migrationContext.Log.Infof("Intercepted changelog state %s", changelogState)
	// 处理changelogState
	switch changelogState {
	case GhostTableMigrated:
		{
			this.ghostTableMigrated <- true
		}
	case AllEventsUpToLockProcessed:
		{
			var applyEventFunc tableWriteFunc = func() error {
				this.allEventsUpToLockProcessed <- changelogStateString
				return nil
			}
			// at this point we know all events up to lock have been read from the streamer,
			// because the streamer works sequentially. So those events are either already handled,
			// or have event functions in applyEventsQueue.
			// So as not to create a potential deadlock, we write this func to applyEventsQueue
			// asynchronously, understanding it doesn't really matter.
			go func() {
				this.applyEventsQueue <- newApplyEventStructByFunc(&applyEventFunc)
			}()
		}
	default:
		{
			return fmt.Errorf("Unknown changelog state: %+v", changelogState)
		}
	}
	this.migrationContext.Log.Infof("Handled changelog state %s", changelogState)
	return nil
}

func (this *Migrator) onChangelogHeartbeatEvent(dmlEvent *binlog.BinlogDMLEvent) (err error) {
	// 拦截心跳表状态，heartbeat的value为时间戳，根据binlog event中对应心跳表的第四列(value列)的ASCII字符串
	changelogHeartbeatString := dmlEvent.NewColumnValues.StringColumn(3)
    // 更新lastHeartbeatOnChangelogTime标记为heartbeatTime
	heartbeatTime, err := time.Parse(time.RFC3339Nano, changelogHeartbeatString)
	if err != nil {
		return this.migrationContext.Log.Errore(err)
	} else {
		this.migrationContext.SetLastHeartbeatOnChangelogTime(heartbeatTime)
		return nil
	}
}

// listenOnPanicAbort aborts on abort request
func (this *Migrator) listenOnPanicAbort() {
	err := <-this.migrationContext.PanicAbort
	this.migrationContext.Log.Fatale(err)
}

// validateStatement validates the `alter` statement meets criteria.
// At this time this means:
// - column renames are approved
// - no table rename allowed
func (this *Migrator) validateStatement() (err error) {
	// 不允许重命名表
	if this.parser.IsRenameTable() {
		return fmt.Errorf("ALTER statement seems to RENAME the table. This is not supported, and you should run your RENAME outside gh-ost.")
	}
	// --approve-renamed-columns 参数下才允许重命名列
	if this.parser.HasNonTrivialRenames() && !this.migrationContext.SkipRenamedColumns {
		// 获取重命名列与源列的对应关系
		this.migrationContext.ColumnRenameMap = this.parser.GetNonTrivialRenames()
		if !this.migrationContext.ApproveRenamedColumns {
			return fmt.Errorf("gh-ost believes the ALTER statement renames columns, as follows: %v; as precaution, you are asked to confirm gh-ost is correct, and provide with `--approve-renamed-columns`, and we're all happy. Or you can skip renamed columns via `--skip-renamed-columns`, in which case column data may be lost", this.parser.GetNonTrivialRenames())
		}
		this.migrationContext.Log.Infof("Alter statement has column(s) renamed. gh-ost finds the following renames: %v; --approve-renamed-columns is given and so migration proceeds.", this.parser.GetNonTrivialRenames())
	}
	// 要删除的列保存在在集合
	this.migrationContext.DroppedColumnsMap = this.parser.DroppedColumnsMap()
	return nil
}

// 统计源表记录数，默认参数配置下不用实际统计，而是使用explain的估算值
func (this *Migrator) countTableRows() (err error) {
	if !this.migrationContext.CountTableRows {
		// Not counting; we stay with an estimate
		return nil
	}
	if this.migrationContext.Noop {
		this.migrationContext.Log.Debugf("Noop operation; not really counting table rows")
		return nil
	}
    // 执行count(*) 获取实际记录数的函数
	countRowsFunc := func() error {
		if err := this.inspector.CountTableRows(); err != nil {
			return err
		}
		if err := this.hooksExecutor.onRowCountComplete(); err != nil {
			return err
		}
		return nil
	}
    // 异步统计实际记录数
	if this.migrationContext.ConcurrentCountTableRows {
		this.migrationContext.Log.Infof("As instructed, counting rows in the background; meanwhile I will use an estimated count, and will update it later on")
		go countRowsFunc()
		// and we ignore errors, because this turns to be a background job
		return nil
	}
	return countRowsFunc()
}

// 如果参数指定PostponeCutOverFlagFile，则创建改文件，将该文件作为信号来推迟cut-over动作；删除该文件时才进行cut-over
func (this *Migrator) createFlagFiles() (err error) {
	if this.migrationContext.PostponeCutOverFlagFile != "" {
		if !base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
			if err := base.TouchFile(this.migrationContext.PostponeCutOverFlagFile); err != nil {
				return this.migrationContext.Log.Errorf("--postpone-cut-over-flag-file indicated by gh-ost is unable to create said file: %s", err.Error())
			}
			this.migrationContext.Log.Infof("Created postpone-cut-over-flag-file: %s", this.migrationContext.PostponeCutOverFlagFile)
		}
	}
	return nil
}

// Migrate executes the complete migration logic. This is *the* major gh-ost function.
func (this *Migrator) Migrate() (err error) {
	this.migrationContext.Log.Infof("Migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	this.migrationContext.StartTime = time.Now()
	if this.migrationContext.Hostname, err = os.Hostname(); err != nil {
		return err
	}

	// 开启一个协程，接收到abort请求后，异常退出
	go this.listenOnPanicAbort()

	// 初始化一个钩子函数
	if err := this.initiateHooksExecutor(); err != nil {
		return err
	}
	// 尝试执行onstartup的钩子
	if err := this.hooksExecutor.onStartup(); err != nil {
		return err
	}
	// 解析alter语句
	if err := this.parser.ParseAlterStatement(this.migrationContext.AlterStatement); err != nil {
		return err
	}
	if err := this.validateStatement(); err != nil {
		return err
	}

	// After this point, we'll need to teardown anything that's been started
	//   so we don't leave things hanging around
	// defer，函数结束时的最后收尾工作
	defer this.teardown()

	// 初始化Inspector
	if err := this.initiateInspector(); err != nil {
		return err
	}
	// 初始化Streamer，模拟从库接收binlog，并注册心跳表_ghc到binlog监听
	if err := this.initiateStreaming(); err != nil {
		return err
	}
	// 初始化Applier，创建心跳表和影子表，修改影子表的表结构，初始化心跳异步协程，写入GhostTableMigrated事件到心跳表
	if err := this.initiateApplier(); err != nil {
		return err
	}
	// 创建flagFile，如果参数指定PostponeCutOverFlagFile，则创建改文件，将该文件作为信号来推迟cut-over动作；删除该文件时才进行cut-over
	if err := this.createFlagFiles(); err != nil {
		return err
	}

	// 通过show slave status获取当前的复制延迟，忽略报错(在主库执行时返回空)
	initialLag, _ := this.inspector.getReplicationLag()
	this.migrationContext.Log.Infof("Waiting for ghost table to be migrated. Current lag is %+v", initialLag)
	// 主程序阻塞，直到接收ghostTableMigrated信道的信号，说明_ghc和_gho表已经准备就绪，继续后面的迁移
	<-this.ghostTableMigrated
	this.migrationContext.Log.Debugf("ghost table migrated")
	// Yay! We now know the Ghost and Changelog tables are good to examine!
	// When running on replica, this means the replica has those tables. When running
	// on master this is always true, of course, and yet it also implies this knowledge
	// is in the binlogs.
	// 对比源表和_gho影子表，确认alter有效，并且获取唯一键和相同列，校验唯一键，
	if err := this.inspector.inspectOriginalAndGhostTables(); err != nil {
		return err
	}
	// Validation complete! We're good to execute this migration
	// 调用onValidated的钩子(如果存在的话)
	if err := this.hooksExecutor.onValidated(); err != nil {
		return err
	}
    // 初始化一个异步server，接收交互式的命令，执行钩子或是更新context的变量值
	if err := this.initiateServer(); err != nil {
		return err
	}
	defer this.server.RemoveSocketFile()

	// 实际统计源表记录数，默认参数配置下使用explain的估算值
	if err := this.countTableRows(); err != nil {
		return err
	}
	// addDMLEventsListener 创建源表binlog监听，并为每个此类事件创建一个写入任务并将其排入队列
	if err := this.addDMLEventsListener(); err != nil {
		return err
	}
	// 获取源表用以拷贝数据的唯一键最小/最大值，rowcopy的范围由此确定
	if err := this.applier.ReadMigrationRangeValues(); err != nil {
		return err
	}
	// initiateThrottler 开始收集和检查限流
	if err := this.initiateThrottler(); err != nil {
		return err
	}
	// 执行onBeforeRowCopy的钩子
	if err := this.hooksExecutor.onBeforeRowCopy(); err != nil {
		return err
	}
	// executeWriteFuncs 通过Applier往_gho表写数据(无论是rowcopy还是binlogevent回放)，binlogevent回放优先，目前是单线程
	go this.executeWriteFuncs()
	// 异步生成拷贝任务，拷贝数据到_gho表，一直执行到拷贝结束
	go this.iterateChunks()
	// MarkRowCopyStartTime 获取当前时间赋值给RowCopyStartTime
	this.migrationContext.MarkRowCopyStartTime()
	// 异步初始化并激活 printStatus() 计时器
	go this.initiateStatus()

	this.migrationContext.Log.Debugf("Operating until row copy is complete")
	// 主进程阻塞，直到接收到rowCopy完成的信号
	this.consumeRowCopyComplete()
	this.migrationContext.Log.Infof("Row copy complete")
	// 执行onRowCopyComplete的钩子
	if err := this.hooksExecutor.onRowCopyComplete(); err != nil {
		return err
	}
	// 打印状态
	this.printStatus(ForcePrintStatusRule)

	// 执行CutOver的钩子
	if err := this.hooksExecutor.onBeforeCutOver(); err != nil {
		return err
	}
	var retrier func(func() error, ...bool) error
	// CutOverExponentialBackoff 每次重试等待指数级时间后，默认为false
	if this.migrationContext.CutOverExponentialBackoff {
		retrier = this.retryOperationWithExponentialBackoff
	} else {
		retrier = this.retryOperation
	}
	// CutOver，表切换
	if err := retrier(this.cutOver); err != nil {
		return err
	}
	atomic.StoreInt64(&this.migrationContext.CutOverCompleteFlag, 1)

	// 收尾工作,删除心跳表，关闭eventStreamer
	if err := this.finalCleanup(); err != nil {
		return nil
	}
	if err := this.hooksExecutor.onSuccess(); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Done migrating %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// ExecOnFailureHook executes the onFailure hook, and this method is provided as the only external
// hook access point
func (this *Migrator) ExecOnFailureHook() (err error) {
	return this.hooksExecutor.onFailure()
}

func (this *Migrator) handleCutOverResult(cutOverError error) (err error) {
	if this.migrationContext.TestOnReplica {
		// We're merely testing, we don't want to keep this state. Rollback the renames as possible
		this.applier.RenameTablesRollback()
	}
	if cutOverError == nil {
		return nil
	}
	// Only on error:

	if this.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		if err := this.hooksExecutor.onStartReplication(); err != nil {
			return this.migrationContext.Log.Errore(err)
		}
		if this.migrationContext.TestOnReplicaSkipReplicaStop {
			this.migrationContext.Log.Warningf("--test-on-replica-skip-replica-stop enabled, we are not starting replication.")
		} else {
			this.migrationContext.Log.Debugf("testing on replica. Starting replication IO thread after cut-over failure")
			if err := this.retryOperation(this.applier.StartReplication); err != nil {
				return this.migrationContext.Log.Errore(err)
			}
		}
	}
	return nil
}

// cutOver performs the final step of migration, based on migration
// type (on replica? atomic? safe?)
func (this *Migrator) cutOver() (err error) {
	if this.migrationContext.Noop {
		this.migrationContext.Log.Debugf("Noop operation; not really swapping tables")
		return nil
	}
	this.migrationContext.MarkPointOfInterest()
	// 判断是否要在切换前限流
	this.throttler.throttle(func() {
		this.migrationContext.Log.Debugf("throttling before swapping tables")
	})

	this.migrationContext.MarkPointOfInterest()
	this.migrationContext.Log.Debugf("checking for cut-over postpone")
	// sleepWhileTrue 无限循环执行func()直到返回false，用来判断heartbeatLag复制延迟、PostponeCutOverFlagFile、UserCommandedUnpostponeFlag等
	this.sleepWhileTrue(
		func() (bool, error) {
			// heartbeatLag 为自上次接收到Heartbeat过去了多长时间
			heartbeatLag := this.migrationContext.TimeSinceLastHeartbeatOnChangelog()
			// MaxLagMillisecondsThrottleThreshold 默认为1500ms
			maxLagMillisecondsThrottle := time.Duration(atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold)) * time.Millisecond
			// CutOverLockTimeoutSeconds 默认为3s
			cutOverLockTimeout := time.Duration(this.migrationContext.CutOverLockTimeoutSeconds) * time.Second
			// 如果心跳的延迟大于 MaxLagMillisecondsThrottleThreshold 或者 CutOverLockTimeoutSeconds ， 则返回true，继续循环
			if heartbeatLag > maxLagMillisecondsThrottle || heartbeatLag > cutOverLockTimeout {
				this.migrationContext.Log.Debugf("current HeartbeatLag (%.2fs) is too high, it needs to be less than both --max-lag-millis (%.2fs) and --cut-over-lock-timeout-seconds (%.2fs) to continue", heartbeatLag.Seconds(), maxLagMillisecondsThrottle.Seconds(), cutOverLockTimeout.Seconds())
				return true, nil
			}
			// 如果 PostponeCutOverFlagFile不存在，则返回false，退出循环
			if this.migrationContext.PostponeCutOverFlagFile == "" {
				return false, nil
			}
			// 如果UserCommandedUnpostponeFlag 即用户交互命令开始cutover，则返回false，退出循环
			if atomic.LoadInt64(&this.migrationContext.UserCommandedUnpostponeFlag) > 0 {
				atomic.StoreInt64(&this.migrationContext.UserCommandedUnpostponeFlag, 0)
				return false, nil
			}
			// 如果暂停CutOverFlagFile存在，则返回true，继续循环
			if base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
				// Postpone file defined and exists!
				if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) == 0 {
					if err := this.hooksExecutor.onBeginPostponed(); err != nil {
						return true, err
					}
				}
				atomic.StoreInt64(&this.migrationContext.IsPostponingCutOver, 1)
				return true, nil
			}
			// 否则返回false，退出循环
			return false, nil
		},
	)
	atomic.StoreInt64(&this.migrationContext.IsPostponingCutOver, 0)
	this.migrationContext.MarkPointOfInterest()
	this.migrationContext.Log.Debugf("checking for cut-over postpone: complete")

	// 如果--test-on-replica,那么在从库进行改表验证，先停复制，切换表名，然后再切换回来
	if this.migrationContext.TestOnReplica {
		// With `--test-on-replica` we stop replication thread, and then proceed to use
		// the same cut-over phase as the master would use. That means we take locks
		// and swap the tables.
		// The difference is that we will later swap the tables back.
		if err := this.hooksExecutor.onStopReplication(); err != nil {
			return err
		}
		if this.migrationContext.TestOnReplicaSkipReplicaStop {
			this.migrationContext.Log.Warningf("--test-on-replica-skip-replica-stop enabled, we are not stopping replication.")
		} else {
			this.migrationContext.Log.Debugf("testing on replica. Stopping replication IO thread")
			// 停掉复制
			if err := this.retryOperation(this.applier.StopReplication); err != nil {
				return err
			}
		}
	}
	// atomic CutOver 原子CutOver
	if this.migrationContext.CutOverType == base.CutOverAtomic {
		// Atomic solution: we use low timeout and multiple attempts. But for
		// each failed attempt, we throttle until replication lag is back to normal
		// atomicCutOver 先创建_del后缀的哨兵表表并锁源表和哨兵表，然后rename源表和影子表，最后删除_del哨兵表并释放源表锁，触发执行rename操作
		err := this.atomicCutOver()
		this.handleCutOverResult(err)
		return err
	}
	// twoStep CutOver
	if this.migrationContext.CutOverType == base.CutOverTwoStep {
		err := this.cutOverTwoStep()
		this.handleCutOverResult(err)
		return err
	}
	return this.migrationContext.Log.Fatalf("Unknown cut-over type: %d; should never get here!", this.migrationContext.CutOverType)
}

// Inject the "AllEventsUpToLockProcessed" state hint, wait for it to appear in the binary logs,
// make sure the queue is drained.
// waitForEventsUpToLock 向心跳表写入AllEventsUpToLockProcessed状态提示，然后等待binlog日志中收到改心跳信息
func (this *Migrator) waitForEventsUpToLock() (err error) {
	// CutOver的timeout时间默认3s
	timeout := time.NewTimer(time.Second * time.Duration(this.migrationContext.CutOverLockTimeoutSeconds))

	this.migrationContext.MarkPointOfInterest()
	waitForEventsUpToLockStartTime := time.Now()

	// 写入心跳表和记录日志 state：AllEventsUpToLockProcessed
	allEventsUpToLockProcessedChallenge := fmt.Sprintf("%s:%d", string(AllEventsUpToLockProcessed), waitForEventsUpToLockStartTime.UnixNano())
	this.migrationContext.Log.Infof("Writing changelog state: %+v", allEventsUpToLockProcessedChallenge)
	if _, err := this.applier.WriteChangelogState(allEventsUpToLockProcessedChallenge); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Waiting for events up to lock")
	atomic.StoreInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 1)
	// CutOver锁表timeout时间内，binlog日志中收到AllEventsUpToLockProcessed的心跳信息，则继续；否则先退出
	for found := false; !found; {
		select {
		case <-timeout.C:
			{
				return this.migrationContext.Log.Errorf("Timeout while waiting for events up to lock")
			}
		case state := <-this.allEventsUpToLockProcessed:
			{
				if state == allEventsUpToLockProcessedChallenge {
					this.migrationContext.Log.Infof("Waiting for events up to lock: got %s", state)
					found = true
				} else {
					this.migrationContext.Log.Infof("Waiting for events up to lock: skipping %s", state)
				}
			}
		}
	}
	waitForEventsUpToLockDuration := time.Since(waitForEventsUpToLockStartTime)

	this.migrationContext.Log.Infof("Done waiting for events up to lock; duration=%+v", waitForEventsUpToLockDuration)
	this.printStatus(ForcePrintStatusAndHintRule)

	return nil
}

// cutOverTwoStep will lock down the original table, execute
// what's left of last DML entries, and **non-atomically** swap original->old, then new->original.
// There is a point in time where the "original" table does not exist and queries are non-blocked
// and failing.
func (this *Migrator) cutOverTwoStep() (err error) {
	atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 0)
	atomic.StoreInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 0)

	if err := this.retryOperation(this.applier.LockOriginalTable); err != nil {
		return err
	}

	if err := this.retryOperation(this.waitForEventsUpToLock); err != nil {
		return err
	}
	if err := this.retryOperation(this.applier.SwapTablesQuickAndBumpy); err != nil {
		return err
	}
	if err := this.retryOperation(this.applier.UnlockTables); err != nil {
		return err
	}

	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	renameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.RenameTablesStartTime)
	this.migrationContext.Log.Debugf("Lock & rename duration: %s (rename only: %s). During this time, queries on %s were locked or failing", lockAndRenameDuration, renameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// atomicCutOver 先创建_del后缀的哨兵表表并锁源表和哨兵表，然后rename源表和影子表，最后删除_del哨兵表并释放源表锁，触发执行rename操作
func (this *Migrator) atomicCutOver() (err error) {
	// CutOver前更新InCutOverCriticalSectionFlag标志，之后恢复标志
	atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 1)
	defer atomic.StoreInt64(&this.migrationContext.InCutOverCriticalSectionFlag, 0)

	okToUnlockTable := make(chan bool, 4)
	// sync.Once 保证函数只被执行一次，在多并发下是线程安全的; 只有Do()一个方法
	var dropCutOverSentryTableOnce sync.Once
	// 函数退出时执行
	defer func() {
		okToUnlockTable <- true
		dropCutOverSentryTableOnce.Do(func() {
			// DropAtomicCutOverSentryTableIfExists 检查_del后缀的哨兵表是否已存在，如果存在则drop该表
			this.applier.DropAtomicCutOverSentryTableIfExists()
		})
	}()

	// 设置context的AllEventsUpToLockProcessedInjectedFlag = 0
	atomic.StoreInt64(&this.migrationContext.AllEventsUpToLockProcessedInjectedFlag, 0)


	lockOriginalSessionIdChan := make(chan int64, 2)
	tableLocked := make(chan error, 2)
	tableUnlocked := make(chan error, 2)
	// 开启一个异步协程，创建_del后缀的额哨兵表，执行锁表(源表和哨兵表)，收到其他会话rename操作成功信号后删除哨兵表，释放源表表锁
	go func() {
		if err := this.applier.AtomicCutOverMagicLock(lockOriginalSessionIdChan, tableLocked, okToUnlockTable, tableUnlocked, &dropCutOverSentryTableOnce); err != nil {
			this.migrationContext.Log.Errore(err)
		}
	}()
	// 获取信道信息，table已经lock
	if err := <-tableLocked; err != nil {
		return this.migrationContext.Log.Errore(err)
	}
	// 记录锁表的会话ID
	lockOriginalSessionId := <-lockOriginalSessionIdChan
	this.migrationContext.Log.Infof("Session locking original & magic tables is %+v", lockOriginalSessionId)
	// At this point we know the original table is locked.
	// We know any newly incoming DML on original table is blocked.
	// waitForEventsUpToLock 向心跳表写入AllEventsUpToLockProcessed状态提示，然后等待binlog日志中收到改心跳信息
	if err := this.waitForEventsUpToLock(); err != nil {
		return this.migrationContext.Log.Errore(err)
	}

	// Step 2
	// We now attempt an atomic RENAME on original & ghost tables, and expect it to block.
	this.migrationContext.RenameTablesStartTime = time.Now()

	var tableRenameKnownToHaveFailed int64
	renameSessionIdChan := make(chan int64, 2)
	tablesRenamed := make(chan error, 2)
	// 开启一个异步协程，执行rename表操作
	go func() {
		if err := this.applier.AtomicCutoverRename(renameSessionIdChan, tablesRenamed); err != nil {
			// Abort! Release the lock
			atomic.StoreInt64(&tableRenameKnownToHaveFailed, 1)
			okToUnlockTable <- true
		}
	}()
	renameSessionId := <-renameSessionIdChan
	this.migrationContext.Log.Infof("Session renaming tables is %+v", renameSessionId)

	waitForRename := func() error {
		if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 1 {
			// We return `nil` here so as to avoid the `retry`. The RENAME has failed,
			// it won't show up in PROCESSLIST, no point in waiting
			return nil
		}
		return this.applier.ExpectProcess(renameSessionId, "metadata lock", "rename")
	}
	// Wait for the RENAME to appear in PROCESSLIST
	if err := this.retryOperation(waitForRename, true); err != nil {
		// Abort! Release the lock
		okToUnlockTable <- true
		return err
	}
	if atomic.LoadInt64(&tableRenameKnownToHaveFailed) == 0 {
		this.migrationContext.Log.Infof("Found atomic RENAME to be blocking, as expected. Double checking the lock is still in place (though I don't strictly have to)")
	}
	// 再次确认lock table依旧持有锁
	if err := this.applier.ExpectUsedLock(lockOriginalSessionId); err != nil {
		// Abort operation. Just make sure to drop the magic table.
		return this.migrationContext.Log.Errore(err)
	}
	this.migrationContext.Log.Infof("Connection holding lock on original table still exists")

	// Now that we've found the RENAME blocking, AND the locking connection still alive,
	// we know it is safe to proceed to release the lock

	// 条件具备，可以drop哨兵表，释放源表表锁了
	// 往信道okToUnlockTable中传入信号，触发drop哨兵表和释放源表表锁
	okToUnlockTable <- true
	// BAM! magic table dropped, original table lock is released
	// -> RENAME released -> queries on original are unblocked.
	if err := <-tableUnlocked; err != nil {
		return this.migrationContext.Log.Errore(err)
	}
	if err := <-tablesRenamed; err != nil {
		return this.migrationContext.Log.Errore(err)
	}
	this.migrationContext.RenameTablesEndTime = time.Now()

	// ooh nice! We're actually truly and thankfully done
	lockAndRenameDuration := this.migrationContext.RenameTablesEndTime.Sub(this.migrationContext.LockTablesStartTime)
	this.migrationContext.Log.Infof("Lock & rename duration: %s. During this time, queries on %s were blocked", lockAndRenameDuration, sql.EscapeName(this.migrationContext.OriginalTableName))
	return nil
}

// initiateServer begins listening on unix socket/tcp for incoming interactive commands
func (this *Migrator) initiateServer() (err error) {
	var f printStatusFunc = func(rule PrintStatusRule, writer io.Writer) {
		// printStatus 输出当前进度
		this.printStatus(rule, writer)
	}
	// 初始化server
	this.server = NewServer(this.migrationContext, this.hooksExecutor, f)
	// 监听socket
	if err := this.server.BindSocketFile(); err != nil {
		return err
	}
	// 监听tcp
	if err := this.server.BindTCPPort(); err != nil {
		return err
	}
    // 异步启动server,处理用户请求,执行对应的钩子函数、更新context的变量值
	go this.server.Serve()
	return nil
}

// initiateInspector connects, validates and inspects the "inspector" server.
// The "inspector" server is typically a replica; it is where we issue some
// queries such as:
// - table row count
// - schema validation
// - heartbeat
// When `--allow-on-master` is supplied, the inspector is actually the master.
// 初始化Inspector
func (this *Migrator) initiateInspector() (err error) {
	this.inspector = NewInspector(this.migrationContext)
	// 初始化Inspector的数据库连接
	if err := this.inspector.InitDBConnections(); err != nil {
		return err
	}
    // validateTable() 源表校验，并获取预估记录数
	if err := this.inspector.ValidateOriginalTable(); err != nil {
		return err
	}
	// // 源表校验（唯一键、列、虚拟列、自增值）
	if err := this.inspector.InspectOriginalTable(); err != nil {
		return err
	}
	// So far so good, table is accessible and valid.
	// Let's get master connection config
	// 获取主库连接信息
	if this.migrationContext.AssumeMasterHostname == "" {
		// No forced master host; detect master
		if this.migrationContext.ApplierConnectionConfig, err = this.inspector.getMasterConnectionConfig(); err != nil {
			return err
		}
		this.migrationContext.Log.Infof("Master found to be %+v", *this.migrationContext.ApplierConnectionConfig.ImpliedKey)
	} else {
		// Forced master host.
		key, err := mysql.ParseInstanceKey(this.migrationContext.AssumeMasterHostname)
		if err != nil {
			return err
		}
		this.migrationContext.ApplierConnectionConfig = this.migrationContext.InspectorConnectionConfig.DuplicateCredentials(*key)
		if this.migrationContext.CliMasterUser != "" {
			this.migrationContext.ApplierConnectionConfig.User = this.migrationContext.CliMasterUser
		}
		if this.migrationContext.CliMasterPassword != "" {
			this.migrationContext.ApplierConnectionConfig.Password = this.migrationContext.CliMasterPassword
		}
		this.migrationContext.Log.Infof("Master forced to be %+v", *this.migrationContext.ApplierConnectionConfig.ImpliedKey)
	}
	// validate configs
	if this.migrationContext.TestOnReplica || this.migrationContext.MigrateOnReplica {
		if this.migrationContext.InspectorIsAlsoApplier() {
			return fmt.Errorf("Instructed to --test-on-replica or --migrate-on-replica, but the server we connect to doesn't seem to be a replica")
		}
		this.migrationContext.Log.Infof("--test-on-replica or --migrate-on-replica given. Will not execute on master %+v but rather on replica %+v itself",
			*this.migrationContext.ApplierConnectionConfig.ImpliedKey, *this.migrationContext.InspectorConnectionConfig.ImpliedKey,
		)
		this.migrationContext.ApplierConnectionConfig = this.migrationContext.InspectorConnectionConfig.Duplicate()
		if this.migrationContext.GetThrottleControlReplicaKeys().Len() == 0 {
			this.migrationContext.AddThrottleControlReplicaKey(this.migrationContext.InspectorConnectionConfig.Key)
		}
	} else if this.migrationContext.InspectorIsAlsoApplier() && !this.migrationContext.AllowedRunningOnMaster {
		return fmt.Errorf("It seems like this migration attempt to run directly on master. Preferably it would be executed on a replica (and this reduces load from the master). To proceed please provide --allow-on-master. Inspector config=%+v, applier config=%+v", this.migrationContext.InspectorConnectionConfig, this.migrationContext.ApplierConnectionConfig)
	}
	// 检查 log_slave_updates 参数设置
	if err := this.inspector.validateLogSlaveUpdates(); err != nil {
		return err
	}

	return nil
}

// initiateStatus sets and activates the printStatus() ticker
func (this *Migrator) initiateStatus() error {
	this.printStatus(ForcePrintStatusAndHintRule)
	statusTick := time.Tick(1 * time.Second)
	for range statusTick {
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return nil
		}
		go this.printStatus(HeuristicPrintStatusRule)
	}

	return nil
}

// printMigrationStatusHint prints a detailed configuration dump, that is useful
// to keep in mind; such as the name of migrated table, throttle params etc.
// This gets printed at beginning and end of migration, every 10 minutes throughout
// migration, and as response to the "status" interactive command.
// printMigrationStatusHint 打印详细配置信息，在迁移开始和结束器和每间隔10分钟，以及响应status命令
func (this *Migrator) printMigrationStatusHint(writers ...io.Writer) {
	w := io.MultiWriter(writers...)
	fmt.Fprintln(w, fmt.Sprintf("# Migrating %s.%s; Ghost table is %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	))
	fmt.Fprintln(w, fmt.Sprintf("# Migrating %+v; inspecting %+v; executing on %+v",
		*this.applier.connectionConfig.ImpliedKey,
		*this.inspector.connectionConfig.ImpliedKey,
		this.migrationContext.Hostname,
	))
	fmt.Fprintln(w, fmt.Sprintf("# Migration started at %+v",
		this.migrationContext.StartTime.Format(time.RubyDate),
	))
	// 获取MaxLoad的参数配置
	maxLoad := this.migrationContext.GetMaxLoad()
	// 获取CriticalLoad的参数配置
	criticalLoad := this.migrationContext.GetCriticalLoad()
	// 打印当前参数配置
	fmt.Fprintln(w, fmt.Sprintf("# chunk-size: %+v; max-lag-millis: %+vms; dml-batch-size: %+v; max-load: %s; critical-load: %s; nice-ratio: %f",
		atomic.LoadInt64(&this.migrationContext.ChunkSize),
		atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold),
		atomic.LoadInt64(&this.migrationContext.DMLBatchSize),
		maxLoad.String(),
		criticalLoad.String(),
		this.migrationContext.GetNiceRatio(),
	))
	// 打印限流文件
	if this.migrationContext.ThrottleFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.ThrottleFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintln(w, fmt.Sprintf("# throttle-flag-file: %+v %+v",
			this.migrationContext.ThrottleFlagFile, setIndicator,
		))
	}
	if this.migrationContext.ThrottleAdditionalFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.ThrottleAdditionalFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintln(w, fmt.Sprintf("# throttle-additional-flag-file: %+v %+v",
			this.migrationContext.ThrottleAdditionalFlagFile, setIndicator,
		))
	}
	// // 打印限流查询
	if throttleQuery := this.migrationContext.GetThrottleQuery(); throttleQuery != "" {
		fmt.Fprintln(w, fmt.Sprintf("# throttle-query: %+v",
			throttleQuery,
		))
	}
	if throttleControlReplicaKeys := this.migrationContext.GetThrottleControlReplicaKeys(); throttleControlReplicaKeys.Len() > 0 {
		fmt.Fprintln(w, fmt.Sprintf("# throttle-control-replicas count: %+v",
			throttleControlReplicaKeys.Len(),
		))
	}
    // 打印延迟切换标志文件
	if this.migrationContext.PostponeCutOverFlagFile != "" {
		setIndicator := ""
		if base.FileExists(this.migrationContext.PostponeCutOverFlagFile) {
			setIndicator = "[set]"
		}
		fmt.Fprintln(w, fmt.Sprintf("# postpone-cut-over-flag-file: %+v %+v",
			this.migrationContext.PostponeCutOverFlagFile, setIndicator,
		))
	}
	// 打印panic标志文件
	if this.migrationContext.PanicFlagFile != "" {
		fmt.Fprintln(w, fmt.Sprintf("# panic-flag-file: %+v",
			this.migrationContext.PanicFlagFile,
		))
	}
	// 最后，打印unix socket和tcp port
	fmt.Fprintln(w, fmt.Sprintf("# Serving on unix socket: %+v",
		this.migrationContext.ServeSocketFile,
	))
	if this.migrationContext.ServeTCPPort != 0 {
		fmt.Fprintln(w, fmt.Sprintf("# Serving on TCP port: %+v", this.migrationContext.ServeTCPPort))
	}
}

// printStatus prints the progress status, and optionally additionally detailed
// dump of configuration.
// `rule` indicates the type of output expected.
// By default the status is written to standard output, but other writers can
// be used as well.
// 输出当前进度
func (this *Migrator) printStatus(rule PrintStatusRule, writers ...io.Writer) {
	if rule == NoPrintStatusRule {
		return
	}
	writers = append(writers, os.Stdout)
    // 从拷贝数据到当前的时间
	elapsedTime := this.migrationContext.ElapsedTime()
	elapsedSeconds := int64(elapsedTime.Seconds())
	// context中copiedrows记录数
	totalRowsCopied := this.migrationContext.GetTotalRowsCopied()
	// rowsEstimate = 表的预估记录数 + 通过binlog回放的记录数
	rowsEstimate := atomic.LoadInt64(&this.migrationContext.RowsEstimate) + atomic.LoadInt64(&this.migrationContext.RowsDeltaEstimate)
	if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 {
		// Done copying rows. The totalRowsCopied value is the de-facto number of rows,
		// and there is no further need to keep updating the value.
		rowsEstimate = totalRowsCopied
	}
	var progressPct float64
	if rowsEstimate == 0 {
		progressPct = 100.0
	} else {
		progressPct = 100.0 * float64(totalRowsCopied) / float64(rowsEstimate)
	}
	// we take the opportunity to update migration context with progressPct
	// 将progressPct更新到context的currentProgress参数
	this.migrationContext.SetProgressPct(progressPct)
	// Before status, let's see if we should print a nice reminder for what exactly we're doing here.
	// 每600s设置printstatus的hint为true
	shouldPrintMigrationStatusHint := (elapsedSeconds%600 == 0)
	if rule == ForcePrintStatusAndHintRule {
		shouldPrintMigrationStatusHint = true
	}
	if rule == ForcePrintStatusOnlyRule {
		shouldPrintMigrationStatusHint = false
	}
	// 打印详细配置信息，在迁移开始和结束器和每间隔10分钟，以及响应status命令
	if shouldPrintMigrationStatusHint {
		this.printMigrationStatusHint(writers...)
	}

	// etaSeconds，flaot64的最大值
	var etaSeconds float64 = math.MaxFloat64
	// etaDuration , int64的最小值(math.MinInt64)
	var etaDuration = time.Duration(base.ETAUnknown)
	if progressPct >= 100.0 {
		etaDuration = 0
	} else if progressPct >= 0.1 {
		// elapsedRowCopySeconds， 从开始拷贝数据到当前的时间(单位：秒)
		elapsedRowCopySeconds := this.migrationContext.ElapsedRowCopyTime().Seconds()
		// totalExpectedSeconds， 总计预估需要的时间
		totalExpectedSeconds := elapsedRowCopySeconds * float64(rowsEstimate) / float64(totalRowsCopied)
		// etaSeconds，剩余拷贝时间
		etaSeconds = totalExpectedSeconds - elapsedRowCopySeconds
		if etaSeconds >= 0 {
			etaDuration = time.Duration(etaSeconds) * time.Second
		} else {
			etaDuration = 0
		}
	}
	// 把预估剩余拷贝时间，更新到context，并格式化复制给eta
	this.migrationContext.SetETADuration(etaDuration)
	var eta string
	switch etaDuration {
	case 0:
		eta = "due"
	case time.Duration(base.ETAUnknown):
		eta = "N/A"
	default:
		eta = base.PrettifyDurationOutput(etaDuration)
	}
    // 更新state状态
	state := "migrating"
	if atomic.LoadInt64(&this.migrationContext.CountingRowsFlag) > 0 && !this.migrationContext.ConcurrentCountTableRows {
		state = "counting rows"
	} else if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
		eta = "due"
		state = "postponing cut-over"
	} else if isThrottled, throttleReason, _ := this.migrationContext.IsThrottled(); isThrottled {
		state = fmt.Sprintf("throttled, %s", throttleReason)
	}

	// 打印state状态的频率
	shouldPrintStatus := false
	if rule == HeuristicPrintStatusRule {
		if elapsedSeconds <= 60 {
			shouldPrintStatus = true
		} else if etaSeconds <= 60 {
			shouldPrintStatus = true
		} else if etaSeconds <= 180 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else if elapsedSeconds <= 180 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else if this.migrationContext.TimeSincePointOfInterest().Seconds() <= 60 {
			shouldPrintStatus = (elapsedSeconds%5 == 0)
		} else {
			shouldPrintStatus = (elapsedSeconds%30 == 0)
		}
	} else {
		// Not heuristic
		shouldPrintStatus = true
	}
	if !shouldPrintStatus {
		return
	}

	// 当前streamer读取的binlog位点
	currentBinlogCoordinates := *this.eventsStreamer.GetCurrentBinlogCoordinates()

	// 打印state状态
	status := fmt.Sprintf("Copy: %d/%d %.1f%%; Applied: %d; Backlog: %d/%d; Time: %+v(total), %+v(copy); streamer: %+v; Lag: %.2fs, HeartbeatLag: %.2fs, State: %s; ETA: %s",
		totalRowsCopied, rowsEstimate, progressPct,
		atomic.LoadInt64(&this.migrationContext.TotalDMLEventsApplied),
		len(this.applyEventsQueue), cap(this.applyEventsQueue),
		base.PrettifyDurationOutput(elapsedTime), base.PrettifyDurationOutput(this.migrationContext.ElapsedRowCopyTime()),
		currentBinlogCoordinates,
		// 当前延迟时间
		this.migrationContext.GetCurrentLagDuration().Seconds(),
		// 上次心跳时间到当前的时间
		this.migrationContext.TimeSinceLastHeartbeatOnChangelog().Seconds(),
		state,
		eta,
	)
	// 拷贝的批次
	this.applier.WriteChangelog(
		fmt.Sprintf("copy iteration %d at %d", this.migrationContext.GetIteration(), time.Now().Unix()),
		status,
	)
	// io.MultiWriter()输出到多个终端，类似于tee命令
	w := io.MultiWriter(writers...)
	fmt.Fprintln(w, status)

	// HooksStatusIntervalSec默认60s，每60s检查执行onStatus的钩子
	if elapsedSeconds%this.migrationContext.HooksStatusIntervalSec == 0 {
		this.hooksExecutor.onStatus(status)
	}
}

// initiateStreaming begins streaming of binary log events and registers listeners for such events
func (this *Migrator) initiateStreaming() error {
	// 实例化EventsStreamer 结构体
	this.eventsStreamer = NewEventsStreamer(this.migrationContext)
	// 获取binlog当前位点，开始接收binlog event
	if err := this.eventsStreamer.InitDBConnections(); err != nil {
		return err
	}
	// 注册listener，监听ghost心跳表_ghc的binlog event
	// AddListener registers a new listener for binlog events, on a per-table basis
	this.eventsStreamer.AddListener(
		false,
		this.migrationContext.DatabaseName,
		// GetChangelogTableName 根据源表名生成_ghc的ghost心跳表
		this.migrationContext.GetChangelogTableName(),
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			// 根据心跳表中hint关键字返回对应的处理函数
			return this.onChangelogEvent(dmlEvent)
		},
	)

	// 异步接收binlog event，获取DmlEvent，根据库表名通知对应的listener
	go func() {
		this.migrationContext.Log.Debugf("Beginning streaming")
		err := this.eventsStreamer.StreamEvents(this.canStopStreaming)
		if err != nil {
			this.migrationContext.PanicAbort <- err
		}
		this.migrationContext.Log.Debugf("Done streaming")
	}()

	// 异步更新context的binlog位点
	go func() {
		ticker := time.Tick(1 * time.Second)
		for range ticker {
			if atomic.LoadInt64(&this.finishedMigrating) > 0 {
				return
			}
			this.migrationContext.SetRecentBinlogCoordinates(*this.eventsStreamer.GetCurrentBinlogCoordinates())
		}
	}()
	return nil
}

// addDMLEventsListener begins listening for binlog events on the original table,
// and creates & enqueues a write task per such event.
// addDMLEventsListener 创建源表binlog监听，并为每个此类事件创建一个写入任务并将其排入队列
func (this *Migrator) addDMLEventsListener() error {
	err := this.eventsStreamer.AddListener(
		false,
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		// 定义一个onDmlEvent，将newApplyEventStructByDML函数处理结果写入队列applyEventsQueue
		func(dmlEvent *binlog.BinlogDMLEvent) error {
			this.applyEventsQueue <- newApplyEventStructByDML(dmlEvent)
			return nil
		},
	)
	return err
}

// initiateThrottler kicks in the throttling collection and the throttling checks.
// initiateThrottler 开始收集和检查限流
func (this *Migrator) initiateThrottler() error {
	// 初始化Throttler
	this.throttler = NewThrottler(this.migrationContext, this.applier, this.inspector)
	// initiateThrottlerCollection 异步收集各个途径的限流指标，并设置context的限流参数
	go this.throttler.initiateThrottlerCollection(this.firstThrottlingCollected)
	this.migrationContext.Log.Infof("Waiting for first throttle metrics to be collected")
	<-this.firstThrottlingCollected // replication lag
	<-this.firstThrottlingCollected // HTTP status
	<-this.firstThrottlingCollected // other, general metrics
	this.migrationContext.Log.Infof("First throttle metrics collected")
	// initiateThrottlerChecks initiates the throttle ticker and sets the basic behavior of throttling.
	go this.throttler.initiateThrottlerChecks()

	return nil
}

// 初始化Applier，创建心跳表和影子表，修改影子表的表结构，初始化心跳异步协程，写入GhostTableMigrated事件到心跳表
func (this *Migrator) initiateApplier() error {
	// 初始化Applier
	this.applier = NewApplier(this.migrationContext)
	// 初始化Applier的DB连接，一个this.db，一个this.singletonDB(singletonDB的最大连接数为1)，获取源表的列
	if err := this.applier.InitDBConnections(); err != nil {
		return err
	}
    // 确认_gho和_old表是否存在，以及是否在开始前删除这两张表
	if err := this.applier.ValidateOrDropExistingTables(); err != nil {
		return err
	}
	// CreateChangelogTable 创建_ghc的心跳表
	if err := this.applier.CreateChangelogTable(); err != nil {
		this.migrationContext.Log.Errorf("Unable to create changelog table, see further error details. Perhaps a previous migration failed without dropping the table? OR is there a running migration? Bailing out")
		return err
	}
	// CreateGhostTable 创建_gho表
	if err := this.applier.CreateGhostTable(); err != nil {
		this.migrationContext.Log.Errorf("Unable to create ghost table, see further error details. Perhaps a previous migration failed without dropping the table? Bailing out")
		return err
	}
    // AlterGhost 修改_gho影子表的表结构
	if err := this.applier.AlterGhost(); err != nil {
		this.migrationContext.Log.Errorf("Unable to ALTER ghost table, see further error details. Bailing out")
		return err
	}
    // 如果源表存在auto_increment，并且ddl语句没有包含auto_increment，那么需要主动拷贝源表的auto_increment到影子表_gho
	if this.migrationContext.OriginalTableAutoIncrement > 0 && !this.parser.IsAutoIncrementDefined() {
		// Original table has AUTO_INCREMENT value and the -alter statement does not indicate any override,
		// so we should copy AUTO_INCREMENT value onto our ghost table.
		if err := this.applier.AlterGhostAutoIncrement(); err != nil {
			this.migrationContext.Log.Errorf("Unable to ALTER ghost table AUTO_INCREMENT value, see further error details. Bailing out")
			return err
		}
	}
	// 写入GhostTableMigrated事件到心跳表
	this.applier.WriteChangelogState(string(GhostTableMigrated))
	// 启动给一个协程，初始化heartbeat，并每隔指定心跳时间写入心跳表
	go this.applier.InitiateHeartbeat()
	return nil
}

// iterateChunks iterates the existing table rows, and generates a copy task of
// a chunk of rows onto the ghost table.
func (this *Migrator) iterateChunks() error {
	// 分批次拷贝数据的失败处理函数 terminateRowIteration，将报错写入信道Migrator.rowCopyComplete
	terminateRowIteration := func(err error) error {
		this.rowCopyComplete <- err
		return this.migrationContext.Log.Errore(err)
	}
	if this.migrationContext.Noop {
		this.migrationContext.Log.Debugf("Noop operation; not really copying data")
		return terminateRowIteration(nil)
	}
	if this.migrationContext.MigrationRangeMinValues == nil {
		this.migrationContext.Log.Debugf("No rows found in table. Rowcopy will be implicitly empty")
		return terminateRowIteration(nil)
	}

	var hasNoFurtherRangeFlag int64
	// 循环每个批次拷贝数据:
	// CalculateNextIterationRangeEndValues 计算下一批次拷贝的起始值
	// 调用applier.ApplyIterationInsertQuery拷贝单个批次的数据到_gho表，返回分批大小、插入行数、SQL执行时间
	for {
		if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 || atomic.LoadInt64(&hasNoFurtherRangeFlag) == 1 {
			// Done
			// There's another such check down the line
			return nil
		}
		copyRowsFunc := func() error {
			if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 || atomic.LoadInt64(&hasNoFurtherRangeFlag) == 1 {
				// Done.
				// There's another such check down the line
				return nil
			}

			// When hasFurtherRange is false, original table might be write locked and CalculateNextIterationRangeEndValues would hangs forever

			hasFurtherRange := false
			// CalculateNextIterationRangeEndValues 计算下一批次拷贝的起始值，赋值给context的MigrationIterationRangeMaxValues，可重试
			if err := this.retryOperation(func() (e error) {
				hasFurtherRange, e = this.applier.CalculateNextIterationRangeEndValues()
				return e
			}); err != nil {
				return terminateRowIteration(err)
			}
			// 如果没有更多批次，则停止拷贝，更新context的hasNoFurtherRangeFlag值
			if !hasFurtherRange {
				atomic.StoreInt64(&hasNoFurtherRangeFlag, 1)
				return terminateRowIteration(nil)
			}
			// Copy task:
			applyCopyRowsFunc := func() error {
				if atomic.LoadInt64(&this.rowCopyCompleteFlag) == 1 {
					// No need for more writes.
					// This is the de-facto place where we avoid writing in the event of completed cut-over.
					// There could _still_ be a race condition, but that's as close as we can get.
					// What about the race condition? Well, there's actually no data integrity issue.
					// when rowCopyCompleteFlag==1 that means **guaranteed** all necessary rows have been copied.
					// But some are still then collected at the binary log, and these are the ones we're trying to
					// not apply here. If the race condition wins over us, then we just attempt to apply onto the
					// _ghost_ table, which no longer exists. So, bothering error messages and all, but no damage.
					return nil
				}
				// 调用applier.ApplyIterationInsertQuery拷贝单个批次的数据到_gho表，返回分批大小、插入行数、SQL执行时间
				_, rowsAffected, _, err := this.applier.ApplyIterationInsertQuery()
				if err != nil {
					return err // wrapping call will retry
				}
				// Go语言的AddInt64()函数用于将增量自动添加到*addr
				atomic.AddInt64(&this.migrationContext.TotalRowsCopied, rowsAffected)
				atomic.AddInt64(&this.migrationContext.Iteration, 1)
				return nil
			}
			// 重试applyCopyRowsFunc批次数据拷贝
			if err := this.retryOperation(applyCopyRowsFunc); err != nil {
				return terminateRowIteration(err)
			}
			return nil
		}
		// Enqueue copy operation; to be executed by executeWriteFuncs()
		// copyRowsFunc 就是实际执行的拷贝数据函数，排入copyRowsQueue队列
		this.copyRowsQueue <- copyRowsFunc
	}
	return nil
}

// onApplyEventStruct 获取applyEventsQueue中的DmlEvent生成回放SQL，应用到_gho表
func (this *Migrator) onApplyEventStruct(eventStruct *applyEventStruct) error {
	// 处理非DMLEvent
	handleNonDMLEventStruct := func(eventStruct *applyEventStruct) error {
		if eventStruct.writeFunc != nil {
			if err := this.retryOperation(*eventStruct.writeFunc); err != nil {
				return this.migrationContext.Log.Errore(err)
			}
		}
		return nil
	}
	if eventStruct.dmlEvent == nil {
		return handleNonDMLEventStruct(eventStruct)
	}
	if eventStruct.dmlEvent != nil {
		dmlEvents := [](*binlog.BinlogDMLEvent){}
		dmlEvents = append(dmlEvents, eventStruct.dmlEvent)
		var nonDmlStructToApply *applyEventStruct

		availableEvents := len(this.applyEventsQueue)
		batchSize := int(atomic.LoadInt64(&this.migrationContext.DMLBatchSize))
		// 计算avaiableEvents=DMLBatchSize，一批次处理的event数量
		if availableEvents > batchSize-1 {
			// The "- 1" is because we already consumed one event: the original event that led to this function getting called.
			// So, if DMLBatchSize==1 we wish to not process any further events
			availableEvents = batchSize - 1
		}
		// dmlEvent 加入到 dmlEvents，dmlEvents最大长度为DMLBatchSize，nonDmlEvent单独处理
		for i := 0; i < availableEvents; i++ {
			additionalStruct := <-this.applyEventsQueue
			if additionalStruct.dmlEvent == nil {
				// Not a DML. We don't group this, and we don't batch any further
				nonDmlStructToApply = additionalStruct
				break
			}
			dmlEvents = append(dmlEvents, additionalStruct.dmlEvent)
		}
		// Create a task to apply the DML event; this will be execute by executeWriteFuncs()
		// dmlEvents一批次一起处理
        // ApplyDMLEventQueries applies multiple DML queries onto the _ghost_ table
		var applyEventFunc tableWriteFunc = func() error {
			return this.applier.ApplyDMLEventQueries(dmlEvents)
		}
		// 处理dmlEvents，失败重试
		if err := this.retryOperation(applyEventFunc); err != nil {
			return this.migrationContext.Log.Errore(err)
		}
		// 处理非DMLEvent
		if nonDmlStructToApply != nil {
			// We pulled DML events from the queue, and then we hit a non-DML event. Wait!
			// We need to handle it!
			if err := handleNonDMLEventStruct(nonDmlStructToApply); err != nil {
				return this.migrationContext.Log.Errore(err)
			}
		}
	}
	return nil
}

// executeWriteFuncs writes data via applier: both the rowcopy and the events backlog.
// This is where the ghost table gets the data. The function fills the data single-threaded.
// Both event backlog and rowcopy events are polled; the backlog events have precedence.
// executeWriteFuncs 通过Applier往_gho表写数据(无论是rowcopy还是binlogevent回放)，binlogevent回放优先，目前是单线程
func (this *Migrator) executeWriteFuncs() error {
	if this.migrationContext.Noop {
		this.migrationContext.Log.Debugf("Noop operation; not really executing write funcs")
		return nil
	}
	// 循环写数据
	for {
		// 判断是否迁移完成
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return nil
		}

		// throttle 根据context的限流参数控制实际限流(连续不断block直到throttle停止)
		this.throttler.throttle(nil)

		// We give higher priority to event processing, then secondary priority to rowCopy
		// select 是 Go 中的一个控制结构，类似于用于通信的 switch 语句。每个 case 必须是一个通信操作，要么是发送要么是接收。
		// select 随机执行一个可运行的 case。如果没有 case 可运行，它将阻塞，直到有 case 可运行。一个默认的子句应该总是可运行的。
		select {
		// 从applyEventsQueue信道中接收eventStruct结构体，信道最大长度由MaxEventsBatchSize决定
		case eventStruct := <-this.applyEventsQueue:
			{
				// onApplyEventStruct 获取applyEventsQueue中的DmlEvent生成回放SQL，应用到_gho表
				if err := this.onApplyEventStruct(eventStruct); err != nil {
					return err
				}
			}
		// 如果没有BinlogEvent，就执行rowCopy
		default:
			{
				select {
				case copyRowsFunc := <-this.copyRowsQueue:
					{
						copyRowsStartTime := time.Now()
						// Retries are handled within the copyRowsFunc
						if err := copyRowsFunc(); err != nil {
							return this.migrationContext.Log.Errore(err)
						}
						// niceRatio 通过控制批量拷贝存量数据的批次之间的执行间隔来控制拷贝数据的速率，例:nice-ratio为0.5，表示增加50%的拷贝时间；nice-ratio为1，表示增加100%的拷贝时间。
						if niceRatio := this.migrationContext.GetNiceRatio(); niceRatio > 0 {
							copyRowsDuration := time.Since(copyRowsStartTime)
							sleepTimeNanosecondFloat64 := niceRatio * float64(copyRowsDuration.Nanoseconds())
							sleepTime := time.Duration(time.Duration(int64(sleepTimeNanosecondFloat64)) * time.Nanosecond)
							time.Sleep(sleepTime)
						}
					}
				default:
					{
						// Hmmmmm... nothing in the queue; no events, but also no row copy.
						// This is possible upon load. Let's just sleep it over.
						this.migrationContext.Log.Debugf("Getting nothing in the write queue. Sleeping...")
						time.Sleep(time.Second)
					}
				}
			}
		}
	}
	return nil
}

// finalCleanup takes actions at very end of migration, dropping tables etc.
// finalCleanup 收尾工作
func (this *Migrator) finalCleanup() error {
	atomic.StoreInt64(&this.migrationContext.CleanupImminentFlag, 1)

	if this.migrationContext.Noop {
		if createTableStatement, err := this.inspector.showCreateTable(this.migrationContext.GetGhostTableName()); err == nil {
			this.migrationContext.Log.Infof("New table structure follows")
			fmt.Println(createTableStatement)
		} else {
			this.migrationContext.Log.Errore(err)
		}
	}
	// 关闭eventsStreamer
	if err := this.eventsStreamer.Close(); err != nil {
		this.migrationContext.Log.Errore(err)
	}

	// 清理心跳表
	if err := this.retryOperation(this.applier.DropChangelogTable); err != nil {
		return err
	}
	if this.migrationContext.OkToDropTable && !this.migrationContext.TestOnReplica {
		if err := this.retryOperation(this.applier.DropOldTable); err != nil {
			return err
		}
	} else {
		if !this.migrationContext.Noop {
			this.migrationContext.Log.Infof("Am not dropping old table because I want this operation to be as live as possible. If you insist I should do it, please add `--ok-to-drop-table` next time. But I prefer you do not. To drop the old table, issue:")
			this.migrationContext.Log.Infof("-- drop table %s.%s", sql.EscapeName(this.migrationContext.DatabaseName), sql.EscapeName(this.migrationContext.GetOldTableName()))
		}
	}
	if this.migrationContext.Noop {
		if err := this.retryOperation(this.applier.DropGhostTable); err != nil {
			return err
		}
	}

	return nil
}

func (this *Migrator) teardown() {
	// 将变量val存储到指针this.finishedMigrating指向的位置，finish标志位置为1
	atomic.StoreInt64(&this.finishedMigrating, 1)

	// 关闭数据库连接
	if this.inspector != nil {
		this.migrationContext.Log.Infof("Tearing down inspector")
		this.inspector.Teardown()
	}

	// 关闭applier的数据库连接，同时设置finish标志位为1
	if this.applier != nil {
		this.migrationContext.Log.Infof("Tearing down applier")
		this.applier.Teardown()
	}

	// 关闭eventstreamer的数据库连接
	if this.eventsStreamer != nil {
		this.migrationContext.Log.Infof("Tearing down streamer")
		this.eventsStreamer.Teardown()
	}

	// 设置finish标志位为1
	if this.throttler != nil {
		this.migrationContext.Log.Infof("Tearing down throttler")
		this.throttler.Teardown()
	}
}
