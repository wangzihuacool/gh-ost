/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/binlog"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	"github.com/outbrain/golib/sqlutils"
	"sync"
)

const (
	atomicCutOverMagicHint = "ghost-cut-over-sentry"
)

type dmlBuildResult struct {
	query     string
	args      []interface{}
	rowsDelta int64
	err       error
}

func newDmlBuildResult(query string, args []interface{}, rowsDelta int64, err error) *dmlBuildResult {
	return &dmlBuildResult{
		query:     query,
		args:      args,
		rowsDelta: rowsDelta,
		err:       err,
	}
}

func newDmlBuildResultError(err error) *dmlBuildResult {
	return &dmlBuildResult{
		err: err,
	}
}

// Applier connects and writes the the applier-server, which is the server where migration
// happens. This is typically the master, but could be a replica when `--test-on-replica` or
// `--execute-on-replica` are given.
// Applier is the one to actually write row data and apply binlog events onto the ghost table.
// It is where the ghost & changelog tables get created. It is where the cut-over phase happens.
type Applier struct {
	connectionConfig  *mysql.ConnectionConfig
	db                *gosql.DB
	singletonDB       *gosql.DB
	migrationContext  *base.MigrationContext
	finishedMigrating int64
	name              string
}

func NewApplier(migrationContext *base.MigrationContext) *Applier {
	return &Applier{
		connectionConfig:  migrationContext.ApplierConnectionConfig,
		migrationContext:  migrationContext,
		finishedMigrating: 0,
		name:              "applier",
	}
}

// 初始化Applier的DB连接，一个this.db，一个this.singletonDB(singletonDB的最大连接数为1)
func (this *Applier) InitDBConnections() (err error) {
	applierUri := this.connectionConfig.GetDBUri(this.migrationContext.DatabaseName)
	if this.db, _, err = mysql.GetDB(this.migrationContext.Uuid, applierUri); err != nil {
		return err
	}
	singletonApplierUri := fmt.Sprintf("%s&timeout=0", applierUri)
	if this.singletonDB, _, err = mysql.GetDB(this.migrationContext.Uuid, singletonApplierUri); err != nil {
		return err
	}
	this.singletonDB.SetMaxOpenConns(1)
	// 校验db连接
	version, err := base.ValidateConnection(this.db, this.connectionConfig, this.migrationContext, this.name)
	if err != nil {
		return err
	}
	if _, err := base.ValidateConnection(this.singletonDB, this.connectionConfig, this.migrationContext, this.name); err != nil {
		return err
	}
	this.migrationContext.ApplierMySQLVersion = version
	// 读取Applier的TimeZone
	if err := this.validateAndReadTimeZone(); err != nil {
		return err
	}
	if !this.migrationContext.AliyunRDS && !this.migrationContext.GoogleCloudPlatform && !this.migrationContext.AzureMySQL {
		if impliedKey, err := mysql.GetInstanceKey(this.db); err != nil {
			return err
		} else {
			this.connectionConfig.ImpliedKey = impliedKey
		}
	}
	// readTableColumns 返回源表的字段列表给Applier
	if err := this.readTableColumns(); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Applier initiated on %+v, version %+v", this.connectionConfig.ImpliedKey, this.migrationContext.ApplierMySQLVersion)
	return nil
}

// validateAndReadTimeZone potentially reads server time-zone
func (this *Applier) validateAndReadTimeZone() error {
	query := `select @@global.time_zone`
	if err := this.db.QueryRow(query).Scan(&this.migrationContext.ApplierTimeZone); err != nil {
		return err
	}

	this.migrationContext.Log.Infof("will use time_zone='%s' on applier", this.migrationContext.ApplierTimeZone)
	return nil
}

// readTableColumns reads table columns on applier
func (this *Applier) readTableColumns() (err error) {
	this.migrationContext.Log.Infof("Examining table structure on applier")
	// GetTableColumns 返回字段列表 和 虚拟列的列表（这里忽略虚拟列）给到 Applier
	this.migrationContext.OriginalTableColumnsOnApplier, _, err = mysql.GetTableColumns(this.db, this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName)
	if err != nil {
		return err
	}
	return nil
}

// showTableStatus returns the output of `show table status like '...'` command
func (this *Applier) showTableStatus(tableName string) (rowMap sqlutils.RowMap) {
	query := fmt.Sprintf(`show /* gh-ost */ table status from %s like '%s'`, sql.EscapeName(this.migrationContext.DatabaseName), tableName)
	sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		rowMap = m
		return nil
	})
	return rowMap
}

// tableExists checks if a given table exists in database
func (this *Applier) tableExists(tableName string) (tableFound bool) {
	m := this.showTableStatus(tableName)
	return (m != nil)
}

// ValidateOrDropExistingTables verifies ghost and changelog tables do not exist,
// or attempts to drop them if instructed to.
func (this *Applier) ValidateOrDropExistingTables() error {
	if this.migrationContext.InitiallyDropGhostTable {
		if err := this.DropGhostTable(); err != nil {
			return err
		}
	}
	if this.tableExists(this.migrationContext.GetGhostTableName()) {
		return fmt.Errorf("Table %s already exists. Panicking. Use --initially-drop-ghost-table to force dropping it, though I really prefer that you drop it or rename it away", sql.EscapeName(this.migrationContext.GetGhostTableName()))
	}
	if this.migrationContext.InitiallyDropOldTable {
		if err := this.DropOldTable(); err != nil {
			return err
		}
	}
	if len(this.migrationContext.GetOldTableName()) > mysql.MaxTableNameLength {
		this.migrationContext.Log.Fatalf("--timestamp-old-table defined, but resulting table name (%s) is too long (only %d characters allowed)", this.migrationContext.GetOldTableName(), mysql.MaxTableNameLength)
	}

	if this.tableExists(this.migrationContext.GetOldTableName()) {
		return fmt.Errorf("Table %s already exists. Panicking. Use --initially-drop-old-table to force dropping it, though I really prefer that you drop it or rename it away", sql.EscapeName(this.migrationContext.GetOldTableName()))
	}

	return nil
}

// CreateGhostTable creates the ghost table on the applier host
func (this *Applier) CreateGhostTable() error {
	query := fmt.Sprintf(`create /* gh-ost */ table %s.%s like %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	this.migrationContext.Log.Infof("Creating ghost table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Ghost table created")
	return nil
}

// AlterGhost applies `alter` statement on ghost table
func (this *Applier) AlterGhost() error {
	query := fmt.Sprintf(`alter /* gh-ost */ table %s.%s %s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		this.migrationContext.AlterStatementOptions,
	)
	this.migrationContext.Log.Infof("Altering ghost table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	)
	this.migrationContext.Log.Debugf("ALTER statement: %s", query)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Ghost table altered")
	return nil
}

// AlterGhost applies `alter` statement on ghost table
func (this *Applier) AlterGhostAutoIncrement() error {
	query := fmt.Sprintf(`alter /* gh-ost */ table %s.%s AUTO_INCREMENT=%d`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		this.migrationContext.OriginalTableAutoIncrement,
	)
	this.migrationContext.Log.Infof("Altering ghost table AUTO_INCREMENT value %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	)
	this.migrationContext.Log.Debugf("AUTO_INCREMENT ALTER statement: %s", query)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Ghost table AUTO_INCREMENT altered")
	return nil
}

// CreateChangelogTable creates the changelog table on the applier host
func (this *Applier) CreateChangelogTable() error {
	if err := this.DropChangelogTable(); err != nil {
		return err
	}
	query := fmt.Sprintf(`create /* gh-ost */ table %s.%s (
			id bigint auto_increment,
			last_update timestamp not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			hint varchar(64) charset ascii not null,
			value varchar(4096) charset ascii not null,
			primary key(id),
			unique key hint_uidx(hint)
		) auto_increment=256
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	this.migrationContext.Log.Infof("Creating changelog table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Changelog table created")
	return nil
}

// dropTable drops a given table on the applied host
func (this *Applier) dropTable(tableName string) error {
	query := fmt.Sprintf(`drop /* gh-ost */ table if exists %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(tableName),
	)
	this.migrationContext.Log.Infof("Dropping table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(tableName),
	)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Table dropped")
	return nil
}

// DropChangelogTable drops the changelog table on the applier host
func (this *Applier) DropChangelogTable() error {
	return this.dropTable(this.migrationContext.GetChangelogTableName())
}

// DropOldTable drops the _Old table on the applier host
func (this *Applier) DropOldTable() error {
	return this.dropTable(this.migrationContext.GetOldTableName())
}

// DropGhostTable drops the ghost table on the applier host
func (this *Applier) DropGhostTable() error {
	return this.dropTable(this.migrationContext.GetGhostTableName())
}

// WriteChangelog writes a value to the changelog table.
// It returns the hint as given, for convenience
func (this *Applier) WriteChangelog(hint, value string) (string, error) {
	explicitId := 0
	switch hint {
	case "heartbeat":
		explicitId = 1
	case "state":
		explicitId = 2
	case "throttle":
		explicitId = 3
	}
	query := fmt.Sprintf(`
			insert /* gh-ost */ into %s.%s
				(id, hint, value)
			values
				(NULLIF(?, 0), ?, ?)
			on duplicate key update
				last_update=NOW(),
				value=VALUES(value)
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetChangelogTableName()),
	)
	_, err := sqlutils.ExecNoPrepare(this.db, query, explicitId, hint, value)
	return hint, err
}

func (this *Applier) WriteAndLogChangelog(hint, value string) (string, error) {
	this.WriteChangelog(hint, value)
	return this.WriteChangelog(fmt.Sprintf("%s at %d", hint, time.Now().UnixNano()), value)
}

func (this *Applier) WriteChangelogState(value string) (string, error) {
	return this.WriteAndLogChangelog("state", value)
}

// InitiateHeartbeat creates a heartbeat cycle, writing to the changelog table.
// This is done asynchronously
func (this *Applier) InitiateHeartbeat() {
	var numSuccessiveFailures int64
	// 定义个injectHeartbeat函数
	injectHeartbeat := func() error {
		if atomic.LoadInt64(&this.migrationContext.HibernateUntil) > 0 {
			return nil
		}
		// 连续写入heartbeat到心跳表失败次数超过defaultretries(即60次)则报错退出
		if _, err := this.WriteChangelog("heartbeat", time.Now().Format(time.RFC3339Nano)); err != nil {
			numSuccessiveFailures++
			if numSuccessiveFailures > this.migrationContext.MaxRetries() {
				return this.migrationContext.Log.Errore(err)
			}
		} else {
			numSuccessiveFailures = 0
		}
		return nil
	}
	// 写入心跳到心跳表
	injectHeartbeat()
    // time.Tick() 函数声明为 Tick(d Duration) <-chan Time
	// 在 golang 中，After(d) 是只等待一次 d 的时长，并在这次等待结束后将当前时间发送到通道。Tick(d) 则是间隔地多次等待，每次等待 d 时长，并在每次间隔结束的时候将当前时间发送到通道。
	// 因为 Tick() 也是在等待结束的时候发送数据到通道，所以它的返回值是一个 channel，从这个 channel 中可读取每次等待完时的时间点。
	heartbeatTick := time.Tick(time.Duration(this.migrationContext.HeartbeatIntervalMilliseconds) * time.Millisecond)

	// 每隔heartbearTick时间间隔，写入心跳表
	// 这里heartbeatTick是channel，每隔指定时间将当前时间写入channel，for range heartbeatTick 判断channel不为空则执行
	for range heartbeatTick {
		if atomic.LoadInt64(&this.finishedMigrating) > 0 {
			return
		}
		// Generally speaking, we would issue a goroutine, but I'd actually rather
		// have this block the loop rather than spam the master in the event something
		// goes wrong
		// 如果限流，则不写心跳表，准确来说啥也不干
		if throttle, _, reasonHint := this.migrationContext.IsThrottled(); throttle && (reasonHint == base.UserCommandThrottleReasonHint) {
			continue
		}
		if err := injectHeartbeat(); err != nil {
			return
		}
	}
}

// ExecuteThrottleQuery executes the `--throttle-query` and returns its results.
func (this *Applier) ExecuteThrottleQuery() (int64, error) {
	throttleQuery := this.migrationContext.GetThrottleQuery()

	if throttleQuery == "" {
		return 0, nil
	}
	var result int64
	if err := this.db.QueryRow(throttleQuery).Scan(&result); err != nil {
		return 0, this.migrationContext.Log.Errore(err)
	}
	return result, nil
}

// ReadMigrationMinValues returns the minimum values to be iterated on rowcopy
func (this *Applier) ReadMigrationMinValues(uniqueKey *sql.UniqueKey) error {
	this.migrationContext.Log.Debugf("Reading migration range according to key: %s", uniqueKey.Name)
    // 构造获取唯一键最小值的SQL
	query, err := sql.BuildUniqueKeyMinValuesPreparedQuery(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, &uniqueKey.Columns)
	if err != nil {
		return err
	}
	rows, err := this.db.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		// MigrationRangeMinValue 为 ColumnValues结构体
		this.migrationContext.MigrationRangeMinValues = sql.NewColumnValues(uniqueKey.Len())
		// SQL查询结构赋值给MigrationRangeMinValues
		if err = rows.Scan(this.migrationContext.MigrationRangeMinValues.ValuesPointers...); err != nil {
			return err
		}
	}
	this.migrationContext.Log.Infof("Migration min values: [%s]", this.migrationContext.MigrationRangeMinValues)

	err = rows.Err()
	return err
}

// ReadMigrationMaxValues returns the maximum values to be iterated on rowcopy
func (this *Applier) ReadMigrationMaxValues(uniqueKey *sql.UniqueKey) error {
	this.migrationContext.Log.Debugf("Reading migration range according to key: %s", uniqueKey.Name)
	// 构造获取唯一键最大值的SQL
	query, err := sql.BuildUniqueKeyMaxValuesPreparedQuery(this.migrationContext.DatabaseName, this.migrationContext.OriginalTableName, &uniqueKey.Columns)
	if err != nil {
		return err
	}
	rows, err := this.db.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		// MigrationRangeMinValue 为 ColumnValues结构体
		this.migrationContext.MigrationRangeMaxValues = sql.NewColumnValues(uniqueKey.Len())
		// SQL查询结构赋值给MigrationRangeMaxValues
		if err = rows.Scan(this.migrationContext.MigrationRangeMaxValues.ValuesPointers...); err != nil {
			return err
		}
	}
	this.migrationContext.Log.Infof("Migration max values: [%s]", this.migrationContext.MigrationRangeMaxValues)

	err = rows.Err()
	return err
}

// ReadMigrationRangeValues reads min/max values that will be used for rowcopy
func (this *Applier) ReadMigrationRangeValues() error {
	// ReadMigrationMinValues 获取唯一键最小值
	if err := this.ReadMigrationMinValues(this.migrationContext.UniqueKey); err != nil {
		return err
	}
	// ReadMigrationMaxValues 获取唯一键最大值
	if err := this.ReadMigrationMaxValues(this.migrationContext.UniqueKey); err != nil {
		return err
	}
	return nil
}

// CalculateNextIterationRangeEndValues reads the next-iteration-range-end unique key values,
// which will be used for copying the next chunk of rows. Ir returns "false" if there is
// no further chunk to work through, i.e. we're past the last chunk and are done with
// iterating the range (and this done with copying row chunks)
// CalculateNextIterationRangeEndValues 计算下一批次拷贝的起始值
func (this *Applier) CalculateNextIterationRangeEndValues() (hasFurtherRange bool, err error) {
	this.migrationContext.MigrationIterationRangeMinValues = this.migrationContext.MigrationIterationRangeMaxValues
	if this.migrationContext.MigrationIterationRangeMinValues == nil {
		this.migrationContext.MigrationIterationRangeMinValues = this.migrationContext.MigrationRangeMinValues
	}
	for i := 0; i < 2; i++ {
		// 构造分批范围的分批下限值查询SQL
		buildFunc := sql.BuildUniqueKeyRangeEndPreparedQueryViaOffset
		if i == 1 {
			// BuildUniqueKeyRangeEndPreparedQueryViaTemptable 构造分批范围的分批下限值查询SQL，与BuildUniqueKeyRangeEndPreparedQueryViaOffset稍有差异
			buildFunc = sql.BuildUniqueKeyRangeEndPreparedQueryViaTemptable
		}
		query, explodedArgs, err := buildFunc(
			this.migrationContext.DatabaseName,
			this.migrationContext.OriginalTableName,
			&this.migrationContext.UniqueKey.Columns,
			this.migrationContext.MigrationIterationRangeMinValues.AbstractValues(),
			this.migrationContext.MigrationRangeMaxValues.AbstractValues(),
			atomic.LoadInt64(&this.migrationContext.ChunkSize),
			this.migrationContext.GetIteration() == 0,
			fmt.Sprintf("iteration:%d", this.migrationContext.GetIteration()),
		)
		if err != nil {
			return hasFurtherRange, err
		}
		// 实际执行查询
		rows, err := this.db.Query(query, explodedArgs...)
		if err != nil {
			return hasFurtherRange, err
		}
		// NewColumnValues 将ColumnValues结构体的abstractValues的值复制到ValuesPointers
		iterationRangeMaxValues := sql.NewColumnValues(this.migrationContext.UniqueKey.Len())
		// rows.Next() 遍历读取结果期，使用rows.Scan()将结果集存到变量。
		// 结果集(rows)未关闭前，底层的连接处于繁忙状态。当遍历读到最后一条记录时，会发生一个内部EOF错误，自动调用rows.Close()。
		// 但是如果提前退出循环，rows不会关闭，连接不会回到连接池中，连接也不会关闭。所以手动关闭非常重要。rows.Close()可以多次调用，是无害操作。
		for rows.Next() {
			// Scan()函数将查询结果赋值给iterationRangeMaxValues.ValuesPointers...
			if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
				return hasFurtherRange, err
			}
			hasFurtherRange = true
		}
		if err = rows.Err(); err != nil {
			return hasFurtherRange, err
		}
		// 如果还有下一批次结果，将当前查询结果赋值给context的 MigrationIterationRangeMaxValues
		if hasFurtherRange {
			this.migrationContext.MigrationIterationRangeMaxValues = iterationRangeMaxValues
			return hasFurtherRange, nil
		}
	}
	this.migrationContext.Log.Debugf("Iteration complete: no further range to iterate")
	return hasFurtherRange, nil
}

// ApplyIterationInsertQuery issues a chunk-INSERT query on the ghost table. It is where
// data actually gets copied from original table.
// ApplyIterationInsertQuery 拷贝单个批次的数据到_gho表，返回分批大小、插入行数、SQL执行时间
func (this *Applier) ApplyIterationInsertQuery() (chunkSize int64, rowsAffected int64, duration time.Duration, err error) {
	startTime := time.Now()
	// 获取context的chunkSize配置，即一个批次拷贝多少条记录
	chunkSize = atomic.LoadInt64(&this.migrationContext.ChunkSize)

	// BuildRangeInsertPreparedQuery 返回分批插入_gho表的插入SQL，这里分批范围是 (rangeMin, rangeMax] ，第一批是 [rangeMin, rangeMax]
	// 最终SQL类似：insert /* gh-ost db.tab1 */ ignore into db._gho (mappedSharedCol1, mappedSharedCol2, mappedSharedCol3)
	//      (select sharedCol1, sharedCol2, sharedCol3 from db.tab1 force index (UniqKeyName)
	//        where (((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)))
	//	        and ((col1 < ?) or ((col1 = ?) and (col2 < ?)) or (((col1 = ?) and (col2 = ?)) and (col3 < ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?))))
	//        lock in share mode)
	query, explodedArgs, err := sql.BuildRangeInsertPreparedQuery(
		this.migrationContext.DatabaseName,
		this.migrationContext.OriginalTableName,
		this.migrationContext.GetGhostTableName(),
		this.migrationContext.SharedColumns.Names(),
		this.migrationContext.MappedSharedColumns.Names(),
		this.migrationContext.UniqueKey.Name,
		&this.migrationContext.UniqueKey.Columns,
		this.migrationContext.MigrationIterationRangeMinValues.AbstractValues(),
		this.migrationContext.MigrationIterationRangeMaxValues.AbstractValues(),
		this.migrationContext.GetIteration() == 0,
		// IsTransactionalTable() 判定表存储引擎是否事务性
		this.migrationContext.IsTransactionalTable(),
	)
	if err != nil {
		return chunkSize, rowsAffected, duration, err
	}

	// 实际执行分批插入SQL
	sqlResult, err := func() (gosql.Result, error) {
		tx, err := this.db.Begin()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		// rowCopy时设置时区为目标库的time_zone，加上NO_AUTO_VALUE_ON_ZERO，和不显式指定时加上STRICT_ALL_TABLES
		sessionQuery := fmt.Sprintf(`SET SESSION time_zone = '%s'`, this.migrationContext.ApplierTimeZone)
		sqlModeAddendum := `,NO_AUTO_VALUE_ON_ZERO`
		if !this.migrationContext.SkipStrictMode {
			sqlModeAddendum = fmt.Sprintf("%s,STRICT_ALL_TABLES", sqlModeAddendum)
		}
		sessionQuery = fmt.Sprintf("%s, sql_mode = CONCAT(@@session.sql_mode, ',%s')", sessionQuery, sqlModeAddendum)

		// rowCopy实际执行插入SQL(insert into ... select)
		if _, err := tx.Exec(sessionQuery); err != nil {
			return nil, err
		}
		result, err := tx.Exec(query, explodedArgs...)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return result, nil
	}()

	if err != nil {
		return chunkSize, rowsAffected, duration, err
	}
	// 获取返回行数和SQL执行时间，debug日志中记录批次最小值、批次最大值、批次、分批大小
	rowsAffected, _ = sqlResult.RowsAffected()
	duration = time.Since(startTime)
	this.migrationContext.Log.Debugf(
		"Issued INSERT on range: [%s]..[%s]; iteration: %d; chunk-size: %d",
		this.migrationContext.MigrationIterationRangeMinValues,
		this.migrationContext.MigrationIterationRangeMaxValues,
		this.migrationContext.GetIteration(),
		chunkSize)
	return chunkSize, rowsAffected, duration, nil
}

// LockOriginalTable places a write lock on the original table
func (this *Applier) LockOriginalTable() error {
	query := fmt.Sprintf(`lock /* gh-ost */ tables %s.%s write`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	this.migrationContext.Log.Infof("Locking %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	this.migrationContext.LockTablesStartTime = time.Now()
	if _, err := sqlutils.ExecNoPrepare(this.singletonDB, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Table locked")
	return nil
}

// UnlockTables makes tea. No wait, it unlocks tables.
func (this *Applier) UnlockTables() error {
	query := `unlock /* gh-ost */ tables`
	this.migrationContext.Log.Infof("Unlocking tables")
	if _, err := sqlutils.ExecNoPrepare(this.singletonDB, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Tables unlocked")
	return nil
}

// SwapTablesQuickAndBumpy issues a two-step swap table operation:
// - rename original table to _old
// - rename ghost table to original
// There is a point in time in between where the table does not exist.
func (this *Applier) SwapTablesQuickAndBumpy() error {
	query := fmt.Sprintf(`alter /* gh-ost */ table %s.%s rename %s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
	)
	this.migrationContext.Log.Infof("Renaming original table")
	this.migrationContext.RenameTablesStartTime = time.Now()
	if _, err := sqlutils.ExecNoPrepare(this.singletonDB, query); err != nil {
		return err
	}
	query = fmt.Sprintf(`alter /* gh-ost */ table %s.%s rename %s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	this.migrationContext.Log.Infof("Renaming ghost table")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.RenameTablesEndTime = time.Now()

	this.migrationContext.Log.Infof("Tables renamed")
	return nil
}

// RenameTablesRollback renames back both table: original back to ghost,
// _old back to original. This is used by `--test-on-replica`
func (this *Applier) RenameTablesRollback() (renameError error) {
	// Restoring tables to original names.
	// We prefer the single, atomic operation:
	query := fmt.Sprintf(`rename /* gh-ost */ table %s.%s to %s.%s, %s.%s to %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	this.migrationContext.Log.Infof("Renaming back both tables")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err == nil {
		return nil
	}
	// But, if for some reason the above was impossible to do, we rename one by one.
	query = fmt.Sprintf(`rename /* gh-ost */ table %s.%s to %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
	)
	this.migrationContext.Log.Infof("Renaming back to ghost table")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		renameError = err
	}
	query = fmt.Sprintf(`rename /* gh-ost */ table %s.%s to %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	this.migrationContext.Log.Infof("Renaming back to original table")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		renameError = err
	}
	return this.migrationContext.Log.Errore(renameError)
}

// StopSlaveIOThread is applicable with --test-on-replica; it stops the IO thread, duh.
// We need to keep the SQL thread active so as to complete processing received events,
// and have them written to the binary log, so that we can then read them via streamer.
func (this *Applier) StopSlaveIOThread() error {
	query := `stop /* gh-ost */ slave io_thread`
	this.migrationContext.Log.Infof("Stopping replication IO thread")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Replication IO thread stopped")
	return nil
}

// StartSlaveIOThread is applicable with --test-on-replica
func (this *Applier) StartSlaveIOThread() error {
	query := `start /* gh-ost */ slave io_thread`
	this.migrationContext.Log.Infof("Starting replication IO thread")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Replication IO thread started")
	return nil
}

// StartSlaveSQLThread is applicable with --test-on-replica
func (this *Applier) StopSlaveSQLThread() error {
	query := `stop /* gh-ost */ slave sql_thread`
	this.migrationContext.Log.Infof("Verifying SQL thread is stopped")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("SQL thread stopped")
	return nil
}

// StartSlaveSQLThread is applicable with --test-on-replica
func (this *Applier) StartSlaveSQLThread() error {
	query := `start /* gh-ost */ slave sql_thread`
	this.migrationContext.Log.Infof("Verifying SQL thread is running")
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("SQL thread started")
	return nil
}

// StopReplication is used by `--test-on-replica` and stops replication.
func (this *Applier) StopReplication() error {
	if err := this.StopSlaveIOThread(); err != nil {
		return err
	}
	if err := this.StopSlaveSQLThread(); err != nil {
		return err
	}
    // GetReplicationBinlogCoordinates 获取slave当前读取和回放的点位
	readBinlogCoordinates, executeBinlogCoordinates, err := mysql.GetReplicationBinlogCoordinates(this.db)
	if err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Replication IO thread at %+v. SQL thread is at %+v", *readBinlogCoordinates, *executeBinlogCoordinates)
	return nil
}

// StartReplication is used by `--test-on-replica` on cut-over failure
func (this *Applier) StartReplication() error {
	if err := this.StartSlaveIOThread(); err != nil {
		return err
	}
	if err := this.StartSlaveSQLThread(); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Replication started")
	return nil
}

// GetSessionLockName returns a name for the special hint session voluntary lock
// GetSessionLockName 返回与会话ID关联的指定锁名 "gh-ost.SessionId.lock"
func (this *Applier) GetSessionLockName(sessionId int64) string {
	return fmt.Sprintf("gh-ost.%d.lock", sessionId)
}

// ExpectUsedLock expects the special hint voluntary lock to exist on given session
func (this *Applier) ExpectUsedLock(sessionId int64) error {
	var result int64
	query := `select is_used_lock(?)`
	lockName := this.GetSessionLockName(sessionId)
	this.migrationContext.Log.Infof("Checking session lock: %s", lockName)
	if err := this.db.QueryRow(query, lockName).Scan(&result); err != nil || result != sessionId {
		return fmt.Errorf("Session lock %s expected to be found but wasn't", lockName)
	}
	return nil
}

// ExpectProcess expects a process to show up in `SHOW PROCESSLIST` that has given characteristics
func (this *Applier) ExpectProcess(sessionId int64, stateHint, infoHint string) error {
	found := false
	query := `
		select id
			from information_schema.processlist
			where
				id != connection_id()
				and ? in (0, id)
				and state like concat('%', ?, '%')
				and info  like concat('%', ?, '%')
	`
	err := sqlutils.QueryRowsMap(this.db, query, func(m sqlutils.RowMap) error {
		found = true
		return nil
	}, sessionId, stateHint, infoHint)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Cannot find process. Hints: %s, %s", stateHint, infoHint)
	}
	return nil
}

// DropAtomicCutOverSentryTableIfExists checks if the "old" table name
// happens to be a cut-over magic table; if so, it drops it.
// DropAtomicCutOverSentryTableIfExists 检查_del后缀的临时表是否已存在，如果存在则drop该表
func (this *Applier) DropAtomicCutOverSentryTableIfExists() error {
	this.migrationContext.Log.Infof("Looking for magic cut-over table")
	// 返回old临时表，命名方式为：_TABLE_del
	tableName := this.migrationContext.GetOldTableName()
	// 查看 "show table status like `_TABLE_del`" 结果，如果结果不存在则返回空
	rowMap := this.showTableStatus(tableName)
	if rowMap == nil {
		// Table does not exist
		return nil
	}
	// 如果返回结果不为空，并且Comment值不是"ghost-cut-over-sentry"，则返回err
	if rowMap["Comment"].String != atomicCutOverMagicHint {
		return fmt.Errorf("Expected magic comment on %s, did not find it", tableName)
	}
	// 如果返回结果不为空，并且Comment是"ghost-cut-over-sentry"，则drop该表
	this.migrationContext.Log.Infof("Dropping magic cut-over table")
	return this.dropTable(tableName)
}

// CreateAtomicCutOverSentryTable 创建_del后缀的哨兵表
func (this *Applier) CreateAtomicCutOverSentryTable() error {
	// 判断如果_del后缀的哨兵表存在，则删除该表
	if err := this.DropAtomicCutOverSentryTableIfExists(); err != nil {
		return err
	}
	tableName := this.migrationContext.GetOldTableName()

	// 创建_del后缀的哨兵表
	query := fmt.Sprintf(`create /* gh-ost */ table %s.%s (
			id int auto_increment primary key
		) engine=%s comment='%s'
		`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(tableName),
		this.migrationContext.TableEngine,
		atomicCutOverMagicHint,
	)
	this.migrationContext.Log.Infof("Creating magic cut-over table %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(tableName),
	)
	if _, err := sqlutils.ExecNoPrepare(this.db, query); err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Magic cut-over table created")

	return nil
}

// AtomicCutOverMagicLock 锁表(源表和哨兵表)
func (this *Applier) AtomicCutOverMagicLock(sessionIdChan chan int64, tableLocked chan<- error, okToUnlockTable <-chan bool, tableUnlocked chan<- error, dropCutOverSentryTableOnce *sync.Once) error {
	tx, err := this.db.Begin()
	if err != nil {
		tableLocked <- err
		return err
	}
	defer func() {
		sessionIdChan <- -1
		tableLocked <- fmt.Errorf("Unexpected error in AtomicCutOverMagicLock(), injected to release blocking channel reads")
		tableUnlocked <- fmt.Errorf("Unexpected error in AtomicCutOverMagicLock(), injected to release blocking channel reads")
		tx.Rollback()
	}()

	// 获取当前DB连接的会话ID，并把会话ID入信道sessionIdChan
	var sessionId int64
	if err := tx.QueryRow(`select connection_id()`).Scan(&sessionId); err != nil {
		tableLocked <- err
		return err
	}
	sessionIdChan <- sessionId

	// 使用MySQL的get_lock(str, 0)函数获取指定名称str的锁
	// (常用于应用程序锁，使用release_lock(str)来获取，在metadata_lock中标记类型为USER LEVEL LOCK)
	lockResult := 0
	query := `select get_lock(?, 0)`
	// GetSessionLockName 返回与会话ID关联的指定锁名 "gh-ost.SessionId.lock"
	lockName := this.GetSessionLockName(sessionId)
	this.migrationContext.Log.Infof("Grabbing voluntary lock: %s", lockName)
	if err := tx.QueryRow(query, lockName).Scan(&lockResult); err != nil || lockResult != 1 {
		err := fmt.Errorf("Unable to acquire lock %s", lockName)
		tableLocked <- err
		return err
	}

	// 设置MySQL会话级lock_wait_timeout时间为CutOverLockTimeoutSeconds参数的2倍
	tableLockTimeoutSeconds := this.migrationContext.CutOverLockTimeoutSeconds * 2
	this.migrationContext.Log.Infof("Setting LOCK timeout as %d seconds", tableLockTimeoutSeconds)
	query = fmt.Sprintf(`set session lock_wait_timeout:=%d`, tableLockTimeoutSeconds)
	if _, err := tx.Exec(query); err != nil {
		tableLocked <- err
		return err
	}

	// CreateAtomicCutOverSentryTable 创建_del后缀的哨兵表
	if err := this.CreateAtomicCutOverSentryTable(); err != nil {
		tableLocked <- err
		return err
	}

	// lock tables TABLE write, _TABLE_del write
	// 同时持有源表和哨兵表的写锁，阻塞rename操作和业务读写操作
	query = fmt.Sprintf(`lock /* gh-ost */ tables %s.%s write, %s.%s write`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
	)
	this.migrationContext.Log.Infof("Locking %s.%s, %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
	)
	this.migrationContext.LockTablesStartTime = time.Now()
	if _, err := tx.Exec(query); err != nil {
		tableLocked <- err
		return err
	}
	this.migrationContext.Log.Infof("Tables locked")
	// 空值入信道tableLocked
	tableLocked <- nil // No error.

	// From this point on, we are committed to UNLOCK TABLES. No matter what happens,
	// the UNLOCK must execute (or, alternatively, this connection dies, which gets the same impact)

	// The cut-over phase will proceed to apply remaining backlog onto ghost table,
	// and issue RENAME. We wait here until told to proceed.

	// 阻塞，直到okToUnlockTable信道获取到消息
	<-okToUnlockTable
	this.migrationContext.Log.Infof("Will now proceed to drop magic table and unlock tables")

	// The magic table is here because we locked it. And we are the only ones allowed to drop it.
	// And in fact, we will:
	// 删除_del后缀的哨兵表
	this.migrationContext.Log.Infof("Dropping magic cut-over table")
	query = fmt.Sprintf(`drop /* gh-ost */ table if exists %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
	)

	dropCutOverSentryTableOnce.Do(func() {
		if _, err := tx.Exec(query); err != nil {
			this.migrationContext.Log.Errore(err)
			// We DO NOT return here because we must `UNLOCK TABLES`!
		}
	})

	// Tables still locked
	// 释放源表的锁
	this.migrationContext.Log.Infof("Releasing lock from %s.%s, %s.%s",
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
	)
	query = `unlock tables`
	if _, err := tx.Exec(query); err != nil {
		tableUnlocked <- err
		return this.migrationContext.Log.Errore(err)
	}
	this.migrationContext.Log.Infof("Tables unlocked")
	tableUnlocked <- nil
	return nil
}

// AtomicCutoverRename 重命名源表和影子表
func (this *Applier) AtomicCutoverRename(sessionIdChan chan int64, tablesRenamed chan<- error) error {
	tx, err := this.db.Begin()
	if err != nil {
		return err
	}
	// 函数退出时回滚
	defer func() {
		tx.Rollback()
		sessionIdChan <- -1
		tablesRenamed <- fmt.Errorf("Unexpected error in AtomicCutoverRename(), injected to release blocking channel reads")
	}()
	// 获取rename表的sessionID
	var sessionId int64
	if err := tx.QueryRow(`select connection_id()`).Scan(&sessionId); err != nil {
		return err
	}
	sessionIdChan <- sessionId

	this.migrationContext.Log.Infof("Setting RENAME timeout as %d seconds", this.migrationContext.CutOverLockTimeoutSeconds)
	query := fmt.Sprintf(`set session lock_wait_timeout:=%d`, this.migrationContext.CutOverLockTimeoutSeconds)
	if _, err := tx.Exec(query); err != nil {
		return err
	}

	// 重命名源表和影子表
	query = fmt.Sprintf(`rename /* gh-ost */ table %s.%s to %s.%s, %s.%s to %s.%s`,
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetOldTableName()),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.GetGhostTableName()),
		sql.EscapeName(this.migrationContext.DatabaseName),
		sql.EscapeName(this.migrationContext.OriginalTableName),
	)
	this.migrationContext.Log.Infof("Issuing and expecting this to block: %s", query)
	if _, err := tx.Exec(query); err != nil {
		tablesRenamed <- err
		return this.migrationContext.Log.Errore(err)
	}
	tablesRenamed <- nil
	this.migrationContext.Log.Infof("Tables renamed")
	return nil
}

// ShowStatusVariable 获取status状态值
func (this *Applier) ShowStatusVariable(variableName string) (result int64, err error) {
	query := fmt.Sprintf(`show global status like '%s'`, variableName)
	if err := this.db.QueryRow(query).Scan(&variableName, &result); err != nil {
		return 0, err
	}
	return result, nil
}

// updateModifiesUniqueKeyColumns checks whether a UPDATE DML event actually
// modifies values of the migration's unique key (the iterated key). This will call
// for special handling.
func (this *Applier) updateModifiesUniqueKeyColumns(dmlEvent *binlog.BinlogDMLEvent) (modifiedColumn string, isModified bool) {
	for _, column := range this.migrationContext.UniqueKey.Columns.Columns() {
		// 列顺序
		tableOrdinal := this.migrationContext.OriginalTableColumns.Ordinals[column.Name]
		// dmlEvent中的前值和后值
		whereColumnValue := dmlEvent.WhereColumnValues.AbstractValues()[tableOrdinal]
		newColumnValue := dmlEvent.NewColumnValues.AbstractValues()[tableOrdinal]
		// 返回更新的唯一键中的列
		if newColumnValue != whereColumnValue {
			return column.Name, true
		}
	}
	return "", false
}

// buildDMLEventQuery creates a query to operate on the ghost table, based on an intercepted binlog
// event entry on the original table.
// buildDMLEventQuery 根据event entry在_gho表上构造相应的回放SQL
func (this *Applier) buildDMLEventQuery(dmlEvent *binlog.BinlogDMLEvent) (results [](*dmlBuildResult)) {
	// dmlEvent *binlog.BinlogDMLEvent is a binary log rows (DML) event entry, with data.
	// dmlEvent.DML代表DML的类型
	// 针对不同的DML类型构造响应的回放SQL
	switch dmlEvent.DML {
	// delete语句
	case binlog.DeleteDML:
		{
		    // BuildDMLDeleteQuery 构造delete语句在_gho表上的回放SQL，仅使用唯一键做条件 (delete回放时仍然是delete)
			query, uniqueKeyArgs, err := sql.BuildDMLDeleteQuery(dmlEvent.DatabaseName, this.migrationContext.GetGhostTableName(), this.migrationContext.OriginalTableColumns, &this.migrationContext.UniqueKey.Columns, dmlEvent.WhereColumnValues.AbstractValues())
			// 为results的slice添加一个dmlBuildResult的结构体，结构体内容就是一个prepared的delete语句，影子表记录数-1(统计用)
			return append(results, newDmlBuildResult(query, uniqueKeyArgs, -1, err))
		}
	// insert语句
	case binlog.InsertDML:
		{
			// BuildDMLInsertQuery 构造insert语句在_gho表上的回放SQL (insert回放时改成replace)
			query, sharedArgs, err := sql.BuildDMLInsertQuery(dmlEvent.DatabaseName, this.migrationContext.GetGhostTableName(), this.migrationContext.OriginalTableColumns, this.migrationContext.SharedColumns, this.migrationContext.MappedSharedColumns, dmlEvent.NewColumnValues.AbstractValues())
			// 为results的slice添加一个dmlBuildResult的结构体，结构体内容就是一个prepared的replace语句，影子表记录数+1(统计用)
			return append(results, newDmlBuildResult(query, sharedArgs, 1, err))
		}
	// update语句
	case binlog.UpdateDML:
		{
		    // 判断唯一键中的列是否被更新；如果更新了唯一键，则将update语句拆分成根据唯一键先delete后insert
			if _, isModified := this.updateModifiesUniqueKeyColumns(dmlEvent); isModified {
				dmlEvent.DML = binlog.DeleteDML
				results = append(results, this.buildDMLEventQuery(dmlEvent)...)
				dmlEvent.DML = binlog.InsertDML
				results = append(results, this.buildDMLEventQuery(dmlEvent)...)
				return results
			}
			// 如果没有更新唯一键，则使用BuildDMLUpdateQuery构造update语句在_gho表上的回放SQL
			query, sharedArgs, uniqueKeyArgs, err := sql.BuildDMLUpdateQuery(dmlEvent.DatabaseName, this.migrationContext.GetGhostTableName(), this.migrationContext.OriginalTableColumns, this.migrationContext.SharedColumns, this.migrationContext.MappedSharedColumns, &this.migrationContext.UniqueKey.Columns, dmlEvent.NewColumnValues.AbstractValues(), dmlEvent.WhereColumnValues.AbstractValues())
			args := sqlutils.Args()
			args = append(args, sharedArgs...)
			args = append(args, uniqueKeyArgs...)
			// 为results的slice添加一个dmlBuildResult的结构体，结构体内容就是一个prepared的update语句，影子表记录数+0(统计用)
			return append(results, newDmlBuildResult(query, args, 0, err))
		}
	}
	return append(results, newDmlBuildResultError(fmt.Errorf("Unknown dml event type: %+v", dmlEvent.DML)))
}

// ApplyDMLEventQueries applies multiple DML queries onto the _ghost_ table
func (this *Applier) ApplyDMLEventQueries(dmlEvents [](*binlog.BinlogDMLEvent)) error {

	var totalDelta int64
    // 开启一个事务批量应用DmlEvents
	err := func() error {
		tx, err := this.db.Begin()
		if err != nil {
			return err
		}

		rollback := func(err error) error {
			tx.Rollback()
			return err
		}

		// 设置time_zone到+00:00
		sessionQuery := "SET SESSION time_zone = '+00:00'"
        // SQL_MODE加上 NO_AUTO_VALUE_ON_ZERO
		sqlModeAddendum := `,NO_AUTO_VALUE_ON_ZERO`
		// 默认情况下，SQL_MODE使用STRICT_ALL_TABLES严格模式
		if !this.migrationContext.SkipStrictMode {
			sqlModeAddendum = fmt.Sprintf("%s,STRICT_ALL_TABLES", sqlModeAddendum)
		}
		// 设置time_zone和sql_mode，sql_mode在现有默认配置基础上加上NO_AUTO_VALUE_ON_ZERO和STRICT_ALL_TABLES
		sessionQuery = fmt.Sprintf("%s, sql_mode = CONCAT(@@session.sql_mode, ',%s')", sessionQuery, sqlModeAddendum)
		if _, err := tx.Exec(sessionQuery); err != nil {
			return rollback(err)
		}
		//
		for _, dmlEvent := range dmlEvents {
			// buildDMLEventQuery根据binlog event生成在影子表上的回放SQL
			for _, buildResult := range this.buildDMLEventQuery(dmlEvent) {
				if buildResult.err != nil {
					return rollback(buildResult.err)
				}
				// 执行回放SQL
				if _, err := tx.Exec(buildResult.query, buildResult.args...); err != nil {
					err = fmt.Errorf("%s; query=%s; args=%+v", err.Error(), buildResult.query, buildResult.args)
					return rollback(err)
				}
				// 影子表记录数变化
				totalDelta += buildResult.rowsDelta
			}
		}
		// 多个回放SQL批量提交
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}()

	if err != nil {
		return this.migrationContext.Log.Errore(err)
	}
	// no error
	// 更新TotalDMLEventsApplied状态值
	atomic.AddInt64(&this.migrationContext.TotalDMLEventsApplied, int64(len(dmlEvents)))
	if this.migrationContext.CountTableRows {
		atomic.AddInt64(&this.migrationContext.RowsDeltaEstimate, totalDelta)
	}
	this.migrationContext.Log.Debugf("ApplyDMLEventQueries() applied %d events in one transaction", len(dmlEvents))
	return nil
}

func (this *Applier) Teardown() {
	this.migrationContext.Log.Debugf("Tearing down...")
	this.db.Close()
	this.singletonDB.Close()
	atomic.StoreInt64(&this.finishedMigrating, 1)
}
