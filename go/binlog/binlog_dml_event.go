/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package binlog

import (
	"fmt"
	"strings"

	"github.com/github/gh-ost/go/sql"
)

type EventDML string

const (
	NotDML    EventDML = "NoDML"
	InsertDML EventDML = "Insert"
	UpdateDML EventDML = "Update"
	DeleteDML EventDML = "Delete"
)

// ToEventDML 判断binlog event entry 中的DML类型
func ToEventDML(description string) EventDML {
	// description can be a statement (`UPDATE my_table ...`) or a RBR event name (`UpdateRowsEventV2`)
	description = strings.TrimSpace(strings.Split(description, " ")[0])
	switch strings.ToLower(description) {
	case "insert":
		return InsertDML
	case "update":
		return UpdateDML
	case "delete":
		return DeleteDML
	}
	if strings.HasPrefix(description, "WriteRows") {
		return InsertDML
	}
	if strings.HasPrefix(description, "UpdateRows") {
		return UpdateDML
	}
	if strings.HasPrefix(description, "DeleteRows") {
		return DeleteDML
	}
	return NotDML
}

// BinlogDMLEvent is a binary log rows (DML) event entry, with data
type BinlogDMLEvent struct {
	DatabaseName      string
	TableName         string
	DML               EventDML
	// WhereColumnValues 即binlog event中的前值
	WhereColumnValues *sql.ColumnValues
	// NewColumnValues 即binlog event中的后值
	NewColumnValues   *sql.ColumnValues
}

// NewBinlogDMLEvent 构造一个BinlogDMLEvent结构体
func NewBinlogDMLEvent(databaseName, tableName string, dml EventDML) *BinlogDMLEvent {
	event := &BinlogDMLEvent{
		DatabaseName: databaseName,
		TableName:    tableName,
		DML:          dml,
	}
	return event
}

// String 返回BinlogDMLEvent结构体中的dml类型和库名、表名
func (this *BinlogDMLEvent) String() string {
	return fmt.Sprintf("[%+v on %s:%s]", this.DML, this.DatabaseName, this.TableName)
}
