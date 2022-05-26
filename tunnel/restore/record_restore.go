package restore

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
)

func BinaryRecordRestore(client *tablestore.TableStoreClient, request *BinaryRecordReplayRequest) (*BinaryRecordReplayResponse, error) {
	records, err := tunnel.UnSerializeBatchBinaryRecordFromBytes(request.Record)
	if err != nil {
		return nil, err
	}

	param := &recordReplayParam{
		client:                    client,
		timestamp:                 request.Timestamp,
		tableName:                 request.TableName,
		discardDataVersion:        request.DiscardDataVersion,
		autoIncrementPKIndex:      request.AutoIncrementPKIndex,
		reGenerateAutoIncrementPK: request.ReGenerateAutoIncrementPK,
	}
	if param.autoIncrementPKIndex < 0 {
		return nil, errors.New("autoIncrementPKIndex can't be less than 0")
	}
	if param.reGenerateAutoIncrementPK && param.autoIncrementPKIndex == 0 {
		return nil, errors.New("autoIncrementPKIndex can't be 0 when autoIncrementPK needs to be regenerated")
	}
	responseInfo, hasTimeoutRecord, recordCount, err := recordReplay(records, param)
	if err != nil {
		return nil, err
	}
	response := &BinaryRecordReplayResponse{
		HasTimeoutRecord:   hasTimeoutRecord,
		RecordRestoreCount: recordCount,
		ResponseInfo:       responseInfo,
	}
	return response, nil
}

func RecordRestore(client *tablestore.TableStoreClient, request *RecordReplayRequest) (*RecordReplayResponse, error) {
	param := &recordReplayParam{
		client:                    client,
		timestamp:                 request.Timestamp,
		tableName:                 request.TableName,
		discardDataVersion:        request.DiscardDataVersion,
		autoIncrementPKIndex:      request.AutoIncrementPKIndex,
		reGenerateAutoIncrementPK: request.ReGenerateAutoIncrementPK,
	}
	if param.autoIncrementPKIndex < 0 {
		return nil, errors.New("autoIncrementPKIndex can't be less than 0")
	}
	if param.reGenerateAutoIncrementPK && param.autoIncrementPKIndex == 0 {
		return nil, errors.New("autoIncrementPKIndex can't be 0 when autoIncrementPK needs to be regenerated")
	}
	responseInfo, hasTimeoutRecord, recordCount, err := recordReplay(request.Record, param)
	if err != nil {
		return nil, err
	}
	response := &RecordReplayResponse{
		HasTimeoutRecord:   hasTimeoutRecord,
		RecordRestoreCount: recordCount,
		ResponseInfo:       responseInfo,
	}
	return response, nil
}
