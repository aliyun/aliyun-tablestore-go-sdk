package restore

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
	"net"
	"reflect"
	"strconv"
)

/*
	when a batch of records contains the same primary key record, that record will be moved to
	the next batch for processing, for example, abacc will be split into abc and ac.
*/
func recordReplay(client *tablestore.TableStoreClient, records []*tunnel.Record, timestamp int64, tableName string, discardDataVersion bool) (ResponseInfo, bool, int, error) {
	var err error
	var cnt int
	var totalLength int
	var hasTimeoutRecord bool
	var responseInfo ResponseInfo
	replayRecords := make([]*tunnel.Record, 0)
	recordMap := make(map[string]bool)
	currentBatch := make([]*tunnel.Record, 0)
	nextBatch := make([]*tunnel.Record, 0)

	for _, record := range records {
		if timestamp != 0 && record.SequenceInfo != nil && record.Timestamp > timestamp {
			hasTimeoutRecord = true
			break
		}
		cnt = processPreviousBatch(cnt, currentBatch, nextBatch, replayRecords, recordMap)
		pkString := convertPkToString(record.PrimaryKey)
		if _, ok := recordMap[pkString]; ok {
			nextBatch = append(nextBatch, record)
			continue
		}
		replayRecords = append(replayRecords, record)
		cnt++
		recordMap[pkString] = true
		if cnt == DefaultBatchWriteRowCount {
			responseInfo, err = executeRecordRestore(client, replayRecords, tableName, discardDataVersion)
			if err != nil {
				return responseInfo, hasTimeoutRecord, 0, err
			}
			totalLength += cnt
			cnt = 0
			recordMap = make(map[string]bool, 0)
			replayRecords = replayRecords[:0]
			currentBatch = nextBatch
			nextBatch = nextBatch[:0]
		}
	}
	//cnt is 0 means that currentBatch has been assigned nextBatch
	if cnt == 0 {
		nextBatch = currentBatch
	}
	responseInfo, cnt, err = processLastBatch(client, replayRecords, nextBatch, tableName, discardDataVersion, responseInfo)
	if err != nil {
		return responseInfo, hasTimeoutRecord, 0, err
	}
	totalLength += cnt
	return responseInfo, hasTimeoutRecord, totalLength, nil
}

func convertPkToString(pks *tunnel.PrimaryKey) string {
	key := ""
	for _, pk := range pks.PrimaryKeys {
		t := reflect.TypeOf(pk.Value)
		switch t.Kind() {
		case reflect.String:
			key += pk.Value.(string)
		case reflect.Int64:
			key += strconv.FormatInt(pk.Value.(int64), 10)
		case reflect.Slice:
			key += string(pk.Value.([]byte))
		default:
			panic(fmt.Errorf("unknown primaryKey type, columnName: %s, value: %v", pk.ColumnName, pk.Value))
		}
	}
	return key
}

func processPreviousBatch(cnt int, currentBatch, nextBatch, replayRecords []*tunnel.Record, recordMap map[string]bool) int {
	if cnt == 0 {
		for _, rec := range currentBatch {
			pkString := convertPkToString(rec.PrimaryKey)
			if recordMap[pkString] {
				nextBatch = append(nextBatch, rec)
			} else {
				cnt++
				recordMap[pkString] = true
				replayRecords = append(replayRecords, rec)
			}
		}
	}
	return cnt
}

func processLastBatch(client *tablestore.TableStoreClient, replayRecords []*tunnel.Record, currentBatch []*tunnel.Record, tableName string, discardDataVersion bool, info ResponseInfo) (ResponseInfo, int, error) {
	var err error
	var totalLength int
	if len(replayRecords) != 0 {
		info, err = executeRecordRestore(client, replayRecords, tableName, discardDataVersion)
		if err != nil {
			return info, 0, err
		}
		totalLength += len(replayRecords)
	}
	for {
		if len(currentBatch) == 0 {
			break
		}
		nextBatch := make([]*tunnel.Record, 0)
		recordMap := make(map[string]bool, 0)
		replayRecords = replayRecords[:0]

		for _, rec := range currentBatch {
			pkString := convertPkToString(rec.PrimaryKey)
			if recordMap[pkString] {
				nextBatch = append(nextBatch, rec)
			} else {
				recordMap[pkString] = true
				replayRecords = append(replayRecords, rec)
			}
		}
		if len(replayRecords) != 0 {
			info, err = executeRecordRestore(client, replayRecords, tableName, discardDataVersion)
			if err != nil {
				return info, 0, err
			}
			totalLength += len(replayRecords)
		}
		currentBatch = nextBatch
	}
	return info, totalLength, nil
}

func executeRecordRestore(client *tablestore.TableStoreClient, records []*tunnel.Record, tableName string, discardDataVersion bool) (ResponseInfo, error) {
	batchWriteReq := genBatchWriteReqForRecordReplay(records, tableName, discardDataVersion)
	batchWriteResp, err := client.BatchWriteRow(batchWriteReq)
	if err != nil {
		return ResponseInfo{}, err
	}
	var retErr = &tablestore.OtsError{}
	var hasFailedRow bool
	for _, result := range batchWriteResp.TableToRowsResult[tableName] {
		if !result.IsSucceed {
			hasFailedRow = true
			retErr.Code = result.Error.Code
			retErr.Message = result.Error.Message
			if retErr.Code == tablestore.STORAGE_TIMEOUT {
				return ResponseInfo{RequestId: batchWriteResp.RequestId}, retErr
			}
		}
	}
	if hasFailedRow {
		return ResponseInfo{RequestId: batchWriteResp.RequestId}, retErr
	}
	return ResponseInfo{RequestId: batchWriteResp.RequestId}, nil
}

func genBatchWriteReqForRecordReplay(records []*tunnel.Record, tableName string, discardDataVersion bool) *tablestore.BatchWriteRowRequest {
	batchWriteReq := new(tablestore.BatchWriteRowRequest)
	for _, rec := range records {
		switch rec.Type {
		case tunnel.AT_Put:
			batchWriteReq.AddRowChange(getPutRowChange(rec, tableName, discardDataVersion))
		case tunnel.AT_Delete:
			batchWriteReq.AddRowChange(getDeleteRowChange(rec, tableName))
		case tunnel.AT_Update:
			batchWriteReq.AddRowChange(getUpdateRowChange(rec, tableName, discardDataVersion))
		}
	}
	return batchWriteReq
}

func getPutRowChange(record *tunnel.Record, tableName string, discardDataVersion bool) *tablestore.PutRowChange {
	putRowChange := new(tablestore.PutRowChange)
	putRowChange.TableName = tableName
	putPk := new(tablestore.PrimaryKey)
	for _, pk := range record.PrimaryKey.PrimaryKeys {
		putPk.AddPrimaryKeyColumn(pk.ColumnName, pk.Value)
	}
	putRowChange.PrimaryKey = putPk
	for _, col := range record.Columns {
		if discardDataVersion || col.Timestamp == nil {
			putRowChange.AddColumn(*col.Name, col.Value)
		} else {
			putRowChange.AddColumnWithTimestamp(*col.Name, col.Value, *col.Timestamp)
		}
	}
	putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	return putRowChange
}

func getDeleteRowChange(record *tunnel.Record, tableName string) *tablestore.DeleteRowChange {
	deleteRowChange := new(tablestore.DeleteRowChange)
	deleteRowChange.TableName = tableName
	deletePk := new(tablestore.PrimaryKey)
	for _, pk := range record.PrimaryKey.PrimaryKeys {
		deletePk.AddPrimaryKeyColumn(pk.ColumnName, pk.Value)
	}
	deleteRowChange.PrimaryKey = deletePk
	deleteRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	return deleteRowChange
}

func getUpdateRowChange(record *tunnel.Record, tableName string, discardDataVersion bool) *tablestore.UpdateRowChange {
	updateRowChange := new(tablestore.UpdateRowChange)
	updateRowChange.TableName = tableName
	updatePk := new(tablestore.PrimaryKey)
	for _, pk := range record.PrimaryKey.PrimaryKeys {
		updatePk.AddPrimaryKeyColumn(pk.ColumnName, pk.Value)
	}
	updateRowChange.PrimaryKey = updatePk
	for _, col := range record.Columns {
		switch col.Type {
		case tunnel.RCT_Put:
			if discardDataVersion || col.Timestamp == nil {
				updateRowChange.PutColumn(*col.Name, col.Value)
			} else {
				updateRowChange.PutColumnWithTimestamp(*col.Name, col.Value, *col.Timestamp)
			}
		case tunnel.RCT_DeleteAllVersions:
			updateRowChange.DeleteColumn(*col.Name)
		case tunnel.RCT_DeleteOneVersion:
			if col.Timestamp != nil {
				updateRowChange.DeleteColumnWithTimestamp(*col.Name, *col.Timestamp)
			}
		}
	}
	updateRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	return updateRowChange
}

func ShouldSleep(err error) bool {
	if err, ok := err.(*tablestore.OtsError); ok && err.Code == tablestore.STORAGE_TIMEOUT {
		return true
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return false
}
