// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package open

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/sink/codec"
	"github.com/flowbehappy/tigate/pkg/sink/codec/internal"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	ticommon "github.com/pingcap/tiflow/pkg/sink/codec/common"
)

type columnsArray []*common.Column

func (a columnsArray) Len() int {
	return len(a)
}

func (a columnsArray) Less(i, j int) bool {
	return a[i].Name < a[j].Name
}

func (a columnsArray) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// sortColumnArrays sort column arrays by name
func sortColumnArrays(arrays ...[]*common.Column) {
	for _, array := range arrays {
		if array != nil {
			sort.Sort(columnsArray(array))
		}
	}
}

type messageRow struct {
	Update     map[string]internal.Column `json:"u,omitempty"`
	PreColumns map[string]internal.Column `json:"p,omitempty"`
	Delete     map[string]internal.Column `json:"d,omitempty"`
}

func (m *messageRow) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *messageRow) decode(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return cerror.WrapError(cerror.ErrUnmarshalFailed, err)
	}
	for colName, column := range m.Update {
		m.Update[colName] = internal.FormatColumn(column)
	}
	for colName, column := range m.Delete {
		m.Delete[colName] = internal.FormatColumn(column)
	}
	for colName, column := range m.PreColumns {
		m.PreColumns[colName] = internal.FormatColumn(column)
	}
	return nil
}

func (m *messageRow) dropNotUpdatedColumns() {
	// if the column is not updated, do not output it.
	for col, value := range m.Update {
		oldValue, ok := m.PreColumns[col]
		if !ok {
			continue
		}
		// sql type is not equal
		if value.Type != oldValue.Type {
			continue
		}
		// value equal
		if codec.IsColumnValueEqual(oldValue.Value, value.Value) {
			delete(m.PreColumns, col)
		}
	}
}

type messageDDL struct {
	Query string             `json:"q"`
	Type  timodel.ActionType `json:"t"`
}

func (m *messageDDL) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *messageDDL) decode(data []byte) error {
	return cerror.WrapError(cerror.ErrUnmarshalFailed, json.Unmarshal(data, m))
}

func newResolvedMessage(ts uint64) *internal.MessageKey {
	return &internal.MessageKey{
		Ts:   ts,
		Type: model.MessageTypeResolved,
	}
}

func rowChangeToMsg(
	e *common.RowChangedEvent,
	config *ticommon.Config,
	largeMessageOnlyHandleKeyColumns bool) (*internal.MessageKey, *messageRow, error) {
	var partition *int64
	if e.TableInfo.IsPartitionTable() {
		tableID := e.GetTableID()
		partition = &tableID
	}
	key := &internal.MessageKey{
		Ts:     e.CommitTs,
		Schema: e.TableInfo.GetSchemaName(),
		Table:  e.TableInfo.GetTableName(),
		//RowID:         e.RowID,
		Partition:     partition,
		Type:          model.MessageTypeRow,
		OnlyHandleKey: largeMessageOnlyHandleKeyColumns,
	}
	value := &messageRow{}
	if e.IsDelete() {
		onlyHandleKeyColumns := config.DeleteOnlyHandleKeyColumns || largeMessageOnlyHandleKeyColumns
		value.Delete = rowChangeColumns2CodecColumns(e.GetPreColumns(), onlyHandleKeyColumns)
		if onlyHandleKeyColumns && len(value.Delete) == 0 {
			return nil, nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found handle key columns for the delete event")
		}
	} else if e.IsUpdate() {
		value.Update = rowChangeColumns2CodecColumns(e.GetColumns(), largeMessageOnlyHandleKeyColumns)
		if config.OpenOutputOldValue {
			value.PreColumns = rowChangeColumns2CodecColumns(e.GetPreColumns(), largeMessageOnlyHandleKeyColumns)
		}
		if largeMessageOnlyHandleKeyColumns && (len(value.Update) == 0 ||
			(len(value.PreColumns) == 0 && config.OpenOutputOldValue)) {
			return nil, nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found handle key columns for the update event")
		}
		if config.OnlyOutputUpdatedColumns {
			value.dropNotUpdatedColumns()
		}
	} else {
		value.Update = rowChangeColumns2CodecColumns(e.GetColumns(), largeMessageOnlyHandleKeyColumns)
		if largeMessageOnlyHandleKeyColumns && len(value.Update) == 0 {
			return nil, nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found handle key columns for the insert event")
		}
	}

	return key, value, nil
}

func msgToRowChange(key *internal.MessageKey, value *messageRow) *common.RowChangedEvent {
	e := new(common.RowChangedEvent)
	// TODO: we lost the startTs from kafka message
	// startTs-based txn filter is out of work
	e.CommitTs = key.Ts

	if len(value.Delete) != 0 {
		preCols := codecColumns2RowChangeColumns(value.Delete)
		sortColumnArrays(preCols)
		indexColumns := common.GetHandleAndUniqueIndexOffsets4Test(preCols)
		e.TableInfo = common.BuildTableInfo(key.Schema, key.Table, preCols, indexColumns)
		e.PreColumns = preCols
	} else {
		cols := codecColumns2RowChangeColumns(value.Update)
		preCols := codecColumns2RowChangeColumns(value.PreColumns)
		sortColumnArrays(cols)
		sortColumnArrays(preCols)
		indexColumns := common.GetHandleAndUniqueIndexOffsets4Test(cols)
		e.TableInfo = common.BuildTableInfo(key.Schema, key.Table, cols, indexColumns)
		e.Columns = cols
		e.PreColumns = preCols
	}

	// TODO: we lost the tableID from kafka message
	if key.Partition != nil {
		e.PhysicalTableID = *key.Partition
		e.TableInfo.TableName.IsPartition = true
	}

	return e
}

func rowChangeColumns2CodecColumns(cols []*common.Column, onlyHandleKeyColumns bool) map[string]internal.Column {
	jsonCols := make(map[string]internal.Column, len(cols))
	for _, col := range cols {
		if col == nil {
			continue
		}
		if onlyHandleKeyColumns && !col.Flag.IsHandleKey() {
			continue
		}
		c := internal.Column{}
		c.FromRowChangeColumn(col)
		jsonCols[col.Name] = c
	}
	if len(jsonCols) == 0 {
		return nil
	}
	return jsonCols
}

func codecColumns2RowChangeColumns(cols map[string]internal.Column) []*common.Column {
	sinkCols := make([]*common.Column, 0, len(cols))
	for name, col := range cols {
		c := col.ToRowChangeColumn(name)
		sinkCols = append(sinkCols, c)
	}
	if len(sinkCols) == 0 {
		return nil
	}
	sort.Slice(sinkCols, func(i, j int) bool {
		return strings.Compare(sinkCols[i].Name, sinkCols[j].Name) > 0
	})
	return sinkCols
}

func ddlEventToMsg(e *model.DDLEvent) (*internal.MessageKey, *messageDDL) {
	key := &internal.MessageKey{
		Ts:     e.CommitTs,
		Schema: e.TableInfo.TableName.Schema,
		Table:  e.TableInfo.TableName.Table,
		Type:   model.MessageTypeDDL,
	}
	value := &messageDDL{
		Query: e.Query,
		Type:  e.Type,
	}
	return key, value
}

func msgToDDLEvent(key *internal.MessageKey, value *messageDDL) *model.DDLEvent {
	e := new(model.DDLEvent)
	e.TableInfo = new(model.TableInfo)
	// TODO: we lost the startTs from kafka message
	// startTs-based txn filter is out of work
	e.CommitTs = key.Ts
	e.TableInfo.TableName = model.TableName{
		Schema: key.Schema,
		Table:  key.Table,
	}
	e.Type = value.Type
	e.Query = value.Query
	return e
}