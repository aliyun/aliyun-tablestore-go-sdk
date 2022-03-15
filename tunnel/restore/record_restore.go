package restore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
)

func BinaryRecordRestore(client *tablestore.TableStoreClient, request *BinaryRecordReplayRequest) (*BinaryRecordReplayResponse, error) {
	records, err := tunnel.UnSerializeBatchBinaryRecordFromBytes(request.Record)
	if err != nil {
		return nil, err
	}
	responseInfo, hasTimeoutRecord, recordCount, err := recordReplay(client, records, request.Timestamp, request.TableName, request.DiscardDataVersion)
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
	responseInfo, hasTimeoutRecord, recordCount, err := recordReplay(client, request.Record, request.Timestamp, request.TableName, request.DiscardDataVersion)
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