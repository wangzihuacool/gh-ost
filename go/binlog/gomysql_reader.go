/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"sync"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/mysql"
	"github.com/github/gh-ost/go/sql"

	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

type GoMySQLReader struct {
	migrationContext         *base.MigrationContext
	connectionConfig         *mysql.ConnectionConfig
	// 引用replication库的结构体，BinlogSyncer syncs binlog event from server.
	// https://github.com/go-mysql-org/go-mysql/blob/master/replication/binlogsyncer.go
	binlogSyncer             *replication.BinlogSyncer
	binlogStreamer           *replication.BinlogStreamer
	currentCoordinates       mysql.BinlogCoordinates
	currentCoordinatesMutex  *sync.Mutex
	LastAppliedRowsEventHint mysql.BinlogCoordinates
}

func NewGoMySQLReader(migrationContext *base.MigrationContext) (binlogReader *GoMySQLReader, err error) {
	binlogReader = &GoMySQLReader{
		migrationContext:        migrationContext,
		connectionConfig:        migrationContext.InspectorConnectionConfig,
		currentCoordinates:      mysql.BinlogCoordinates{},
		currentCoordinatesMutex: &sync.Mutex{},
		binlogSyncer:            nil,
		binlogStreamer:          nil,
	}

	// 模拟从库的server_id
	serverId := uint32(migrationContext.ReplicaServerId)

	// 引用replication库的结构体，BinlogSyncerConfig is the configuration for BinlogSyncer.
	// https://github.com/go-mysql-org/go-mysql/blob/master/replication/binlogsyncer.go
	binlogSyncerConfig := replication.BinlogSyncerConfig{
		ServerID:   serverId,
		Flavor:     "mysql",
		Host:       binlogReader.connectionConfig.Key.Hostname,
		Port:       uint16(binlogReader.connectionConfig.Key.Port),
		User:       binlogReader.connectionConfig.User,
		Password:   binlogReader.connectionConfig.Password,
		TLSConfig:  binlogReader.connectionConfig.TLSConfig(),
		// Use decimal.Decimal structure for decimals.
		UseDecimal: true,
	}
	// 创建一个binlogSyncer接收binlog，引用replication库的函数。
	// https://github.com/go-mysql-org/go-mysql/blob/master/replication/binlogsyncer.go
	binlogReader.binlogSyncer = replication.NewBinlogSyncer(binlogSyncerConfig)
    // 返回replication库的BinlogSyncer结构体的一个实例
	return binlogReader, err
}

// ConnectBinlogStreamer 开始接收binlogSteam
func (this *GoMySQLReader) ConnectBinlogStreamer(coordinates mysql.BinlogCoordinates) (err error) {
	if coordinates.IsEmpty() {
		return this.migrationContext.Log.Errorf("Empty coordinates at ConnectBinlogStreamer()")
	}

	this.currentCoordinates = coordinates
	this.migrationContext.Log.Infof("Connecting binlog streamer at %+v", this.currentCoordinates)
	// Start sync with specified binlog file and position
	this.binlogStreamer, err = this.binlogSyncer.StartSync(gomysql.Position{this.currentCoordinates.LogFile, uint32(this.currentCoordinates.LogPos)})

	return err
}

func (this *GoMySQLReader) GetCurrentBinlogCoordinates() *mysql.BinlogCoordinates {
	this.currentCoordinatesMutex.Lock()
	defer this.currentCoordinatesMutex.Unlock()
	returnCoordinates := this.currentCoordinates
	return &returnCoordinates
}

// handleRowsEvent 处理RowsEvent，写入信道entriesChannel
func (this *GoMySQLReader) handleRowsEvent(ev *replication.BinlogEvent, rowsEvent *replication.RowsEvent, entriesChannel chan<- *BinlogEntry) error {
	if this.currentCoordinates.SmallerThanOrEquals(&this.LastAppliedRowsEventHint) {
		this.migrationContext.Log.Debugf("Skipping handled query at %+v", this.currentCoordinates)
		return nil
	}

	dml := ToEventDML(ev.Header.EventType.String())
	if dml == NotDML {
		return fmt.Errorf("Unknown DML type: %s", ev.Header.EventType.String())
	}
	for i, row := range rowsEvent.Rows {
		if dml == UpdateDML && i%2 == 1 {
			// An update has two rows (WHERE+SET)
			// We do both at the same time
			continue
		}
		binlogEntry := NewBinlogEntryAt(this.currentCoordinates)
		binlogEntry.DmlEvent = NewBinlogDMLEvent(
			string(rowsEvent.Table.Schema),
			string(rowsEvent.Table.Table),
			dml,
		)
		switch dml {
		case InsertDML:
			{
				binlogEntry.DmlEvent.NewColumnValues = sql.ToColumnValues(row)
			}
		case UpdateDML:
			{
				binlogEntry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
				binlogEntry.DmlEvent.NewColumnValues = sql.ToColumnValues(rowsEvent.Rows[i+1])
			}
		case DeleteDML:
			{
				binlogEntry.DmlEvent.WhereColumnValues = sql.ToColumnValues(row)
			}
		}
		// The channel will do the throttling. Whoever is reading from the channel
		// decides whether action is taken synchronously (meaning we wait before
		// next iteration) or asynchronously (we keep pushing more events)
		// In reality, reads will be synchronous
		entriesChannel <- binlogEntry
	}
	this.LastAppliedRowsEventHint = this.currentCoordinates
	return nil
}

// StreamEvents 处理binlog event，获取DMLEvent entry，调用handleRowsEvent将EventEntries写入entriesChannel
func (this *GoMySQLReader) StreamEvents(canStopStreaming func() bool, entriesChannel chan<- *BinlogEntry) error {
	if canStopStreaming() {
		return nil
	}
	for {
		if canStopStreaming() {
			break
		}
		// binlogStreamer.GetEvent gets the binlog event one by one,it will block until Syncer receives any events from MySQL
		// You can pass a context (like Cancel or Timeout) to break the block
		// context.Background() 返回一个空的context，这里传一个空的context给GetEvent()函数
		// ev 即返回的BinlogEvent
		ev, err := this.binlogStreamer.GetEvent(context.Background())
		if err != nil {
			return err
		}
		func() {
			this.currentCoordinatesMutex.Lock()
			defer this.currentCoordinatesMutex.Unlock()
			// 获取下一个event的位点
			this.currentCoordinates.LogPos = int64(ev.Header.LogPos)
		}()
		// rotateEvent处理，读取下一个binlog
		if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
			func() {
				this.currentCoordinatesMutex.Lock()
				defer this.currentCoordinatesMutex.Unlock()
				// 下一个event的logfile
				this.currentCoordinates.LogFile = string(rotateEvent.NextLogName)
			}()
			this.migrationContext.Log.Infof("rotate to next log from %s:%d to %s", this.currentCoordinates.LogFile, int64(ev.Header.LogPos), rotateEvent.NextLogName)
		// 处理RowsEvent，将binlogEntry写入信道entriesChannel
		} else if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
			if err := this.handleRowsEvent(ev, rowsEvent, entriesChannel); err != nil {
				return err
			}
		}
	}
	this.migrationContext.Log.Debugf("done streaming events")

	return nil
}

func (this *GoMySQLReader) Close() error {
	this.binlogSyncer.Close()
	return nil
}
