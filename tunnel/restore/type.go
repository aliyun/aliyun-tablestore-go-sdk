package restore

import "github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"

const DefaultBatchWriteRowCount = 200

type RecordReplayRequest struct {
	Record []*tunnel.Record
	//stream record end timestamp, if the record exceeds this timestamp, it won't be replayed.
	//when timestamp is 0, all records will be replayed.
	Timestamp          int64
	TableName          string
	DiscardDataVersion bool //whether to discard data version
}

type RecordReplayResponse struct {
	RecordRestoreCount int
	HasTimeoutRecord   bool //whether to include record that exceeds timestamp in recordReplayRequest
	ResponseInfo
}

type BinaryRecordReplayRequest struct {
	Record             []byte
	Timestamp          int64
	TableName          string
	DiscardDataVersion bool
}

type BinaryRecordReplayResponse struct {
	RecordRestoreCount int
	HasTimeoutRecord   bool
	ResponseInfo
}

type ResponseInfo struct {
	RequestId string
}
