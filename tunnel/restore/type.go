package restore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
)

const DefaultBatchWriteRowCount = 200

type RecordReplayRequest struct {
	Record []*tunnel.Record
	//stream record end timestamp, if the record exceeds this timestamp, it won't be replayed.
	//when timestamp is 0, all records will be replayed.
	Timestamp            int64
	TableName            string
	DiscardDataVersion   bool //whether to discard data version
	AutoIncrementPKIndex int  //the index of the autoIncrement pk column
	//whether the server needs to regenerate the autoIncrement column
	ReGenerateAutoIncrementPK bool
}

type RecordReplayResponse struct {
	RecordRestoreCount int
	HasTimeoutRecord   bool //whether to include record that exceeds timestamp in recordReplayRequest
	ResponseInfo
}

type BinaryRecordReplayRequest struct {
	Record []byte
	//stream record end timestamp, if the record exceeds this timestamp, it won't be replayed.
	//when timestamp is 0, all records will be replayed.
	Timestamp            int64
	TableName            string
	DiscardDataVersion   bool //whether to discard data version
	AutoIncrementPKIndex int  //The index of the autoIncrement pk column
	//whether the server needs to regenerate the autoIncrement column
	ReGenerateAutoIncrementPK bool
}

type BinaryRecordReplayResponse struct {
	RecordRestoreCount int
	HasTimeoutRecord   bool
	ResponseInfo
}

type ResponseInfo struct {
	RequestId string
}

type recordReplayParam struct {
	client                    *tablestore.TableStoreClient
	timestamp                 int64
	tableName                 string
	discardDataVersion        bool //whether to discard data version
	autoIncrementPKIndex      int
	reGenerateAutoIncrementPK bool
}
