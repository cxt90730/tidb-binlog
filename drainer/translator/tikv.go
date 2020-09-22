package translator

import (
	types "github.com/pingcap/tidb/types"
	"io"
	"time"

	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-binlog/pkg/util"
	"github.com/pingcap/tidb/tablecodec"
	tipb "github.com/pingcap/tipb/go-binlog"
	"github.com/pingcap/log"
)

const (
	TableBuckets           = "buckets"
	TableUsers             = "users"
	TableObjects           = "objects"
	TableObjectPart        = "objectpart"
	TableMultiParts        = "multiparts"
	TableParts             = "multipartpart"
	TableClusters          = "cluster"
	TableGc                = "gc"
	TableRestore           = "restoreobjects" // freezer
	TableRestoreObjectPart = "restoreobjectpart"
	TableHotObjects        = "hotobjects"
	TableQos               = "qos"
	TableLifeCycle         = "lifecycle"

	DefaultDatabase      = "yig"
	DefaultCaddyDatabase = "caddy"
)

func newRef(table string) (ref interface{}) {
	switch table {
	case TableBuckets:
		return new(meta.Bucket)
	case TableHotObjects, TableObjects:
		return new(meta.Object)
	case TableLifeCycle:
		return new(meta.LifeCycle)
	case TableMultiParts:
		return new(meta.Multipart)
	case TableClusters:
		return new(meta.Cluster)
	case TableObjectPart, TableRestoreObjectPart, TableParts:
		return new(meta.Part)
	case TableRestore:
		return new(meta.Freezer)
	case TableUsers:
		return new(meta.Users)
	default:
		return nil
	}
}

// convert the tikv
func TiBinlogToTiKV(infoGetter TableInfoGetter, schema string, table string, tiBinlog *tipb.Binlog, pv *tipb.PrewriteValue) (ref interface{}, err error) {
	for _, mut := range pv.GetMutations() {
		var info *model.TableInfo
		var ok bool
		info, ok = infoGetter.TableByID(mut.GetTableId())
		if !ok {
			return nil, errors.Errorf("TableByID empty table id: %d", mut.GetTableId())
		}

		isTblDroppingCol := infoGetter.IsDroppingColumn(mut.GetTableId())

		schema, table, ok = infoGetter.SchemaAndTableName(mut.GetTableId())
		if !ok {
			return nil, errors.Errorf("SchemaAndTableName empty table id: %d", mut.GetTableId())
		}

		iter := newSequenceIterator(&mut)
		for {
			mutType, row, err := iter.next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, errors.Trace(err)
			}

			switch mutType {
			case tipb.MutationType_Insert:
				event, err := handleInsert(schema, info, row)
				if err != nil {
					return nil, errors.Annotatef(err, "genInsert failed")
				}

			case tipb.MutationType_Update:
				event, err := handleUpdate(schema, info, row, isTblDroppingCol)
				if err != nil {
					return nil, errors.Annotatef(err, "genUpdate failed")
				}

			case tipb.MutationType_DeleteRow:
				event, err := handleDelete(schema, info, row)
				if err != nil {
					return nil, errors.Annotatef(err, "genDelete failed")
				}


			default:
				return nil, errors.Errorf("unknown mutation type: %v", mutType)
			}
		}
	}
	return
}

type TikvEvent struct {
	SchemaName       string
	TableName        string
	Tp               EventType
	Row              []byte
	ref              interface{}
}

type EventType int32

const (
	EventType_Insert EventType = 0
	EventType_Update EventType = 1
	EventType_Delete EventType = 2
)

func handleInsert(schema string, table *model.TableInfo, row []byte) (event *TikvEvent, err error) {
	columns := table.Columns

	_, columnValues, err := insertRowToDatums(table, row)
	if err != nil {
		return nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
	}

	var (
		vals       = make([]types.Datum, 0, len(columns))
		cols       = make([]string, 0, len(columns))
		tps        = make([]byte, 0, len(columns))
		mysqlTypes = make([]string, 0, len(columns))
	)

	for _, col := range columns {
		cols = append(cols, col.Name.O)
		tps = append(tps, col.Tp)
		mysqlTypes = append(mysqlTypes, types.TypeToStr(col.Tp, col.Charset))
		val, ok := columnValues[col.ID]
		if !ok {
			val = getDefaultOrZeroValue(col)
		}

		value, err := formatData(val, col.FieldType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		vals = append(vals, value)
	}


	event = packTikvEvent(schema, table.Name.O, EventType_Insert, nil)

	return
}

func handleUpdate(schema string, table *model.TableInfo, row []byte, isTblDroppingCol bool) (event *TikvEvent, err error) {
	columns := writableColumns(table)
	colsMap := util.ToColumnMap(columns)

	oldColumnValues, newColumnValues, err := DecodeOldAndNewRow(row, colsMap, time.Local, isTblDroppingCol)
	if err != nil {
		return nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
	}

	var (
		oldVals    = make([]types.Datum, 0, len(columns))
		newVals    = make([]types.Datum, 0, len(columns))
		cols       = make([]string, 0, len(columns))
		tps        = make([]byte, 0, len(columns))
		mysqlTypes = make([]string, 0, len(columns))
	)
	for _, col := range columns {
		val, ok := newColumnValues[col.ID]
		if ok {
			oldValue, err := formatData(oldColumnValues[col.ID], col.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newValue, err := formatData(val, col.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			oldVals = append(oldVals, oldValue)
			newVals = append(newVals, newValue)
			cols = append(cols, col.Name.O)
			tps = append(tps, col.Tp)
			mysqlTypes = append(mysqlTypes, types.TypeToStr(col.Tp, col.Charset))
		}
	}


	event = packTikvEvent(schema, table.Name.O, EventType_Update, nil)

	return
}

func handleDelete(schema string, table *model.TableInfo, row []byte) (event *TikvEvent, err error) {
	columns := table.Columns
	colsTypeMap := util.ToColumnTypeMap(columns)

	columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, time.Local)
	if err != nil {
		return nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
	}

	var (
		vals       = make([]types.Datum, 0, len(columns))
		cols       = make([]string, 0, len(columns))
		tps        = make([]byte, 0, len(columns))
		mysqlTypes = make([]string, 0, len(columns))
	)
	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			value, err := formatData(val, col.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			vals = append(vals, value)
			cols = append(cols, col.Name.O)
			tps = append(tps, col.Tp)
			mysqlTypes = append(mysqlTypes, types.TypeToStr(col.Tp, col.Charset))
		}
	}

	event = packTikvEvent(schema, table.Name.O, EventType_Delete, nil)

	return
}

func packTikvEvent(schemaName, tableName string, tp EventType, rowData []byte) *TikvEvent {
	event := &TikvEvent{
		SchemaName: schemaName,
		TableName:  tableName,
		Row:        rowData,
		Tp:         tp,
	}

	return event
}