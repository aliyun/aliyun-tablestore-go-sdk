package restore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/testConfig"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
	. "gopkg.in/check.v1"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type BackupRestoreSuite struct{}

var tableNamePrefix string

var _ = Suite(&BackupRestoreSuite{})

var client *tablestore.TableStoreClient

func (s *BackupRestoreSuite) SetUpSuite(c *C) {
	endpoint := testConfig.OtsEndpoint
	instanceName := testConfig.InstanceName
	accessKeyId := testConfig.OtsAccessId
	accessKeySecret := testConfig.OtsAccessKey

	client = tablestore.NewClient(endpoint, instanceName, accessKeyId, accessKeySecret)

	tableNamePrefix = strings.Replace(runtime.Version(), ".", "", -1)
}

func (s *BackupRestoreSuite) Test_RecordRestore(c *C) {
	tableName := tableNamePrefix + strconv.Itoa(time.Now().Nanosecond())
	delTableReq := &tablestore.DeleteTableRequest{TableName: tableName}
	err := prepareRecordTable(tableName)
	c.Assert(err, IsNil)
	time.Sleep(time.Second * 2)
	defer client.DeleteTable(delTableReq)
	columnName := "col"
	recordTimestamp := time.Now().Unix() * 1000
	tests := []struct {
		name               string
		request            *RecordReplayRequest
		recordRestoreCount int
		hasTimeoutRecord   bool
		wantErr            bool
	}{
		{
			name: "have pk conflict, expect write success",
			request: &RecordReplayRequest{
				Record: []*tunnel.Record{
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Delete,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(1),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Update,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
				},
				Timestamp:          0,
				TableName:          tableName,
				DiscardDataVersion: false,
			},
			recordRestoreCount: 4,
			hasTimeoutRecord:   false,
			wantErr:            false,
		},
		{
			name: "set timestamp, expect partial write success",
			request: &RecordReplayRequest{
				Record: []*tunnel.Record{
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp + 1,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Delete,
						Timestamp:    recordTimestamp + 2,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(1),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Update,
						Timestamp:    recordTimestamp + 3,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
				},
				Timestamp:          recordTimestamp + 2,
				TableName:          tableName,
				DiscardDataVersion: false,
			},
			recordRestoreCount: 3,
			hasTimeoutRecord:   true,
			wantErr:            false,
		},
		{
			name: "exist pk is inconsistent, expect write fail",
			request: &RecordReplayRequest{
				Record: []*tunnel.Record{
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp + 1,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Delete,
						Timestamp:    recordTimestamp + 2,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(1),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Update,
						Timestamp:    recordTimestamp + 3,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
				},
				Timestamp:          recordTimestamp + 2,
				TableName:          tableName,
				DiscardDataVersion: false,
			},
			recordRestoreCount: 0,
			hasTimeoutRecord:   false,
			wantErr:            true,
		},
	}

	for _, t := range tests {
		resp, err := RecordRestore(client, t.request)
		if t.wantErr {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(resp.RecordRestoreCount, Equals, t.recordRestoreCount)
		c.Assert(resp.HasTimeoutRecord, Equals, t.hasTimeoutRecord)
	}
}

//test 5000 rows with the same pk
func (s *BackupRestoreSuite) Test_RecordRestore_BatchWithSamePk(c *C) {
	tableName := tableNamePrefix + strconv.Itoa(time.Now().Nanosecond())
	delTableReq := &tablestore.DeleteTableRequest{TableName: tableName}
	err := prepareRecordTable(tableName)
	c.Assert(err, IsNil)
	time.Sleep(time.Second * 2)
	defer client.DeleteTable(delTableReq)
	columnName := "col"
	recordTimestamp := time.Now().Unix() * 1000
	request := &RecordReplayRequest{
		TableName:          tableName,
		DiscardDataVersion: true,
	}
	row := &tablestore.Row{
		PrimaryKey: &tablestore.PrimaryKey{
			PrimaryKeys: []*tablestore.PrimaryKeyColumn{
				{
					ColumnName: "PkString",
					Value:      "pk1",
				},
				{
					ColumnName: "PkInt",
					Value:      int64(0),
				},
				{
					ColumnName: "PkBinary",
					Value:      []byte("pkBinary"),
				},
			},
		},
		Columns: []*tablestore.AttributeColumn{
			{
				ColumnName: columnName,
				Value:      int64(4999),
			},
		},
	}
	records := make([]*tunnel.Record, 0)

	for i := 0; i < 5000; i++ {
		record := &tunnel.Record{
			Type: tunnel.AT_Put,
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{
						ColumnName: "PkString",
						Value:      "pk1",
					},
					{
						ColumnName: "PkInt",
						Value:      int64(0),
					},
					{
						ColumnName: "PkBinary",
						Value:      []byte("pkBinary"),
					},
				},
			},
			Columns: []*tunnel.RecordColumn{
				{
					Type:      tunnel.RCT_Put,
					Name:      &columnName,
					Value:     int64(i),
					Timestamp: &recordTimestamp,
				},
			},
		}
		records = append(records, record)
	}
	request.Record = records
	resp, err := RecordRestore(client, request)
	c.Assert(err, IsNil)
	c.Assert(resp.RecordRestoreCount, Equals, 5000)
	c.Assert(resp.HasTimeoutRecord, Equals, false)
	got := getRangeRestoreTable(client, tableName, c)
	c.Assert(len(got), Equals, 1)
	want := []*tablestore.Row{row}
	compareSameKeyRestoreRows(want, got, c)
}

//test 5000 rows with different pk
func (s *BackupRestoreSuite) Test_RecordRestore_BatchWithDiffPk(c *C) {
	tableName := tableNamePrefix + strconv.Itoa(time.Now().Nanosecond())
	delTableReq := &tablestore.DeleteTableRequest{TableName: tableName}
	err := prepareRecordTable(tableName)
	c.Assert(err, IsNil)
	time.Sleep(time.Second * 2)
	defer client.DeleteTable(delTableReq)

	columnName := "col"
	recordTimestamp := time.Now().Unix() * 1000
	request := &RecordReplayRequest{
		TableName:          tableName,
		DiscardDataVersion: true,
	}
	records := make([]*tunnel.Record, 0)
	want := make([]*tablestore.Row, 0)

	for i := 0; i < 5000; i++ {
		row := &tablestore.Row{
			PrimaryKey: &tablestore.PrimaryKey{
				PrimaryKeys: []*tablestore.PrimaryKeyColumn{
					{
						ColumnName: "PkString",
						Value:      "pk1",
					},
					{
						ColumnName: "PkInt",
						Value:      int64(i),
					},
					{
						ColumnName: "PkBinary",
						Value:      []byte("pkBinary"),
					},
				},
			},
			Columns: []*tablestore.AttributeColumn{
				{
					ColumnName: columnName,
					Value:      int64(i),
				},
			},
		}
		want = append(want, row)

		record := &tunnel.Record{
			Type: tunnel.AT_Put,
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{
						ColumnName: "PkString",
						Value:      "pk1",
					},
					{
						ColumnName: "PkInt",
						Value:      int64(i),
					},
					{
						ColumnName: "PkBinary",
						Value:      []byte("pkBinary"),
					},
				},
			},
			Columns: []*tunnel.RecordColumn{
				{
					Type:      tunnel.RCT_Put,
					Name:      &columnName,
					Value:     int64(i),
					Timestamp: &recordTimestamp,
				},
			},
		}
		records = append(records, record)
	}
	request.Record = records
	resp, err := RecordRestore(client, request)
	c.Assert(err, IsNil)
	c.Assert(resp.RecordRestoreCount, Equals, 5000)
	c.Assert(resp.HasTimeoutRecord, Equals, false)
	got := getRangeRestoreTable(client, tableName, c)
	c.Assert(len(got), Equals, 5000)
	compareDiffKeyRestoreRows(want, got, c)
}

//test 5000 rows with the same pk and different pk
func (s *BackupRestoreSuite) Test_RecordRestore_BatchWithFuzzyPk(c *C) {
	tableName := tableNamePrefix + strconv.Itoa(time.Now().Nanosecond())
	delTableReq := &tablestore.DeleteTableRequest{TableName: tableName}
	err := prepareRecordTable(tableName)
	c.Assert(err, IsNil)
	time.Sleep(time.Second * 2)
	defer client.DeleteTable(delTableReq)

	columnName := "col"
	recordTimestamp := time.Now().Unix() * 1000
	request := &RecordReplayRequest{
		TableName:          tableName,
		DiscardDataVersion: true,
	}
	records := make([]*tunnel.Record, 0)
	want := make([]*tablestore.Row, 0)

	var i int
	for j := 0; j < 5000; j++ {
		if j%2 != 0 {
			i = j - 1
		} else {
			i = j
			row := &tablestore.Row{
				PrimaryKey: &tablestore.PrimaryKey{
					PrimaryKeys: []*tablestore.PrimaryKeyColumn{
						{
							ColumnName: "PkString",
							Value:      "pk1",
						},
						{
							ColumnName: "PkInt",
							Value:      int64(i),
						},
						{
							ColumnName: "PkBinary",
							Value:      []byte("pkBinary"),
						},
					},
				},
				Columns: []*tablestore.AttributeColumn{
					{
						ColumnName: columnName,
						Value:      int64(i),
					},
				},
			}
			want = append(want, row)
		}
		record := &tunnel.Record{
			Type: tunnel.AT_Put,
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{
						ColumnName: "PkString",
						Value:      "pk1",
					},
					{
						ColumnName: "PkInt",
						Value:      int64(i),
					},
					{
						ColumnName: "PkBinary",
						Value:      []byte("pkBinary"),
					},
				},
			},
			Columns: []*tunnel.RecordColumn{
				{
					Type:      tunnel.RCT_Put,
					Name:      &columnName,
					Value:     int64(i),
					Timestamp: &recordTimestamp,
				},
			},
		}
		records = append(records, record)
	}
	request.Record = records
	resp, err := RecordRestore(client, request)
	c.Assert(err, IsNil)
	c.Assert(resp.RecordRestoreCount, Equals, 5000)
	c.Assert(resp.HasTimeoutRecord, Equals, false)
	rows := getRangeRestoreTable(client, tableName, c)
	c.Assert(len(rows), Equals, 2500)
	compareFuzzyRestoreRows(want, rows, c)
}

//test for the first batch is exactly 200 rows, and the second batch need processed by processLastBatch
func (s *BackupRestoreSuite) Test_RecordRestore_WithTwiceBatchWrite(c *C) {
	tableName := tableNamePrefix + strconv.Itoa(time.Now().Nanosecond())
	delTableReq := &tablestore.DeleteTableRequest{TableName: tableName}
	err := prepareRecordTable(tableName)
	c.Assert(err, IsNil)
	time.Sleep(time.Second * 2)
	defer client.DeleteTable(delTableReq)

	columnName := "col"
	recordTimestamp := time.Now().Unix() * 1000
	request := &RecordReplayRequest{
		TableName:          tableName,
		DiscardDataVersion: true,
	}
	records := make([]*tunnel.Record, 0)
	want := make([]*tablestore.Row, 0)

	for i := 0; i < 200; i++ {
		row := &tablestore.Row{
			PrimaryKey: &tablestore.PrimaryKey{
				PrimaryKeys: []*tablestore.PrimaryKeyColumn{
					{
						ColumnName: "PkString",
						Value:      "pk1",
					},
					{
						ColumnName: "PkInt",
						Value:      int64(i),
					},
					{
						ColumnName: "PkBinary",
						Value:      []byte("pkBinary"),
					},
				},
			},
			Columns: []*tablestore.AttributeColumn{
				{
					ColumnName: columnName,
					Value:      int64(i),
				},
			},
		}
		want = append(want, row)

		record := &tunnel.Record{
			Type: tunnel.AT_Put,
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{
						ColumnName: "PkString",
						Value:      "pk1",
					},
					{
						ColumnName: "PkInt",
						Value:      int64(i),
					},
					{
						ColumnName: "PkBinary",
						Value:      []byte("pkBinary"),
					},
				},
			},
			Columns: []*tunnel.RecordColumn{
				{
					Type:      tunnel.RCT_Put,
					Name:      &columnName,
					Value:     int64(i),
					Timestamp: &recordTimestamp,
				},
			},
		}
		records = append(records, record)
		if i >= 0 && i <= 1 {
			records = append(records, record)
		}
	}
	request.Record = records
	resp, err := RecordRestore(client, request)
	c.Assert(err, IsNil)
	c.Assert(resp.RecordRestoreCount, Equals, 202)
	c.Assert(resp.HasTimeoutRecord, Equals, false)
	got := getRangeRestoreTable(client, tableName, c)
	c.Assert(len(got), Equals, 200)
	compareDiffKeyRestoreRows(want, got, c)
}

//The server regenerate the value of the autoIncrement pk column. update and delete operations will be ignored.
func (s *BackupRestoreSuite) Test_AutoIncrementPk_RecordRestore_WithRegenerate(c *C) {
	tableName := tableNamePrefix + strconv.Itoa(time.Now().Nanosecond())
	delTableReq := &tablestore.DeleteTableRequest{TableName: tableName}
	tableMeta, err := prepareAutoIncPkTable(tableName)
	c.Assert(err, IsNil)
	c.Assert(GetAutoIncrementPkIndex(tableMeta), Equals, 1)
	time.Sleep(time.Second * 2)
	defer client.DeleteTable(delTableReq)

	columnName := "col"
	recordTimestamp := time.Now().Unix() * 1000
	tests := []struct {
		name               string
		request            *RecordReplayRequest
		recordRestoreCount int
		hasTimeoutRecord   bool
		wantErr            bool
	}{
		{
			name: "",
			request: &RecordReplayRequest{
				Record: []*tunnel.Record{
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Delete,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(1),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Update,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
				},
				Timestamp:                 0,
				TableName:                 tableName,
				DiscardDataVersion:        false,
				AutoIncrementPKIndex:      GetAutoIncrementPkIndex(tableMeta),
				ReGenerateAutoIncrementPK: true,
			},
			recordRestoreCount: 4,
			hasTimeoutRecord:   false,
			wantErr:            false,
		},
	}

	for _, t := range tests {
		resp, err := RecordRestore(client, t.request)
		if t.wantErr {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		want := make([]*tablestore.Row, 0)
		for _, r := range t.request.Record {
			if r.Type == tunnel.AT_Put {
				want = append(want, convertRecordsToRow(r))
			}
		}
		c.Assert(resp.RecordRestoreCount, Equals, t.recordRestoreCount)
		c.Assert(resp.HasTimeoutRecord, Equals, t.hasTimeoutRecord)
		got := getRangeRestoreTable(client, tableName, c)
		c.Assert(len(got), Equals, 2)
		compareRestoreRowsIncludeAutoInc(want, got, t.request.AutoIncrementPKIndex, c)
	}
}

//The server retains the value of the autoIncrement pk column
func (s *BackupRestoreSuite) Test_AutoIncrementPk_RecordRestore_WithoutRegenerate(c *C) {
	tableName := tableNamePrefix + strconv.Itoa(time.Now().Nanosecond())
	delTableReq := &tablestore.DeleteTableRequest{TableName: tableName}
	err := prepareRecordTable(tableName)
	c.Assert(err, IsNil)
	autoIncTableName := tableName + "autoIncrement"
	_, err = prepareAutoIncPkTable(autoIncTableName)
	c.Assert(err, IsNil)
	delAutoIncTableReq := &tablestore.DeleteTableRequest{TableName: autoIncTableName}
	time.Sleep(time.Second * 2)
	defer client.DeleteTable(delTableReq)
	defer client.DeleteTable(delAutoIncTableReq)

	columnName := "col"
	recordTimestamp := time.Now().Unix() * 1000
	tests := []struct {
		name               string
		request            *RecordReplayRequest
		recordRestoreCount int
		hasTimeoutRecord   bool
		wantErr            bool
		expectRows         []*tablestore.Row
		errCode            string
	}{
		{
			name:       "restore to a table without autoIncrementPK, expect success",
			expectRows: getExpectRowsForAutoIncWithoutRegen(columnName),
			request: &RecordReplayRequest{
				Record: []*tunnel.Record{
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(1),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(2),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Delete,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Update,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(1),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal1",
								Timestamp: &recordTimestamp,
							},
						},
					},
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(2),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal1",
								Timestamp: &recordTimestamp,
							},
						},
					},
				},

				Timestamp:                 0,
				TableName:                 tableName,
				DiscardDataVersion:        false,
				AutoIncrementPKIndex:      0,
				ReGenerateAutoIncrementPK: false,
			},
			recordRestoreCount: 6,
			hasTimeoutRecord:   false,
			wantErr:            false,
		},
		{
			name: "restore to a table with autoIncrementPK, expect failed",
			request: &RecordReplayRequest{
				Record: []*tunnel.Record{
					{
						Type:         tunnel.AT_Put,
						Timestamp:    recordTimestamp,
						SequenceInfo: &tunnel.SequenceInfo{},
						PrimaryKey: &tunnel.PrimaryKey{
							PrimaryKeys: []*tunnel.PrimaryKeyColumn{
								{
									ColumnName: "PkString",
									Value:      "pk1",
								},
								{
									ColumnName: "PkInt",
									Value:      int64(0),
								},
								{
									ColumnName: "PkBinary",
									Value:      []byte("pkBinary"),
								},
							},
						},
						Columns: []*tunnel.RecordColumn{
							{
								Type:      tunnel.RCT_Put,
								Name:      &columnName,
								Value:     "colVal",
								Timestamp: &recordTimestamp,
							},
						},
					},
				},

				Timestamp:                 0,
				TableName:                 autoIncTableName,
				DiscardDataVersion:        false,
				AutoIncrementPKIndex:      0,
				ReGenerateAutoIncrementPK: false,
			},
			wantErr: true,
			errCode: "OTSConditionCheckFail",
		},
	}

	for _, t := range tests {
		resp, err := RecordRestore(client, t.request)
		if t.wantErr {
			if otsErr, ok := err.(*tablestore.OtsError); ok {
				c.Assert(otsErr.Code, Equals, t.errCode)
			}
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(resp.RecordRestoreCount, Equals, t.recordRestoreCount)
		c.Assert(resp.HasTimeoutRecord, Equals, t.hasTimeoutRecord)
		got := getRangeRestoreTable(client, tableName, c)
		c.Assert(len(got), Equals, 2)
		compareRestoreRowsIncludeAutoInc(t.expectRows, got, t.request.AutoIncrementPKIndex, c)
	}
}

func prepareAutoIncPkTable(tableName string) (*tablestore.TableMeta, error) {
	createTableRequest := new(tablestore.CreateTableRequest)
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("PkString", tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumnOption("PkInt", tablestore.PrimaryKeyType_INTEGER, tablestore.AUTO_INCREMENT)
	tableMeta.AddPrimaryKeyColumn("PkBinary", tablestore.PrimaryKeyType_BINARY)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput
	_, err := client.CreateTable(createTableRequest)
	return tableMeta, err
}

func prepareRecordTable(tableName string) error {
	createTableRequest := new(tablestore.CreateTableRequest)
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("PkString", tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("PkInt", tablestore.PrimaryKeyType_INTEGER)
	tableMeta.AddPrimaryKeyColumn("PkBinary", tablestore.PrimaryKeyType_BINARY)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput
	_, err := client.CreateTable(createTableRequest)
	return err
}

func convertRecordsToRow(record *tunnel.Record) *tablestore.Row {
	row := new(tablestore.Row)
	primaryKey := new(tablestore.PrimaryKey)
	for _, pk := range record.PrimaryKey.PrimaryKeys {
		otsPk := &tablestore.PrimaryKeyColumn{
			ColumnName: pk.ColumnName,
			Value:      pk.Value,
		}
		primaryKey.PrimaryKeys = append(primaryKey.PrimaryKeys, otsPk)
	}
	row.PrimaryKey = primaryKey

	attrCol := make([]*tablestore.AttributeColumn, 0)
	for _, colVal := range record.Columns {
		otsCol := &tablestore.AttributeColumn{
			ColumnName: *colVal.Name,
			Value:      colVal.Value,
		}
		attrCol = append(attrCol, otsCol)
	}
	row.Columns = attrCol
	return row
}

func buildGetRangeReq(tableName string) *tablestore.GetRangeRequest {
	getRangeRequest := &tablestore.GetRangeRequest{}
	rangeRowQueryCriteria := &tablestore.RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = tableName
	startPK := new(tablestore.PrimaryKey)
	startPK.AddPrimaryKeyColumnWithMinValue("PkString")
	startPK.AddPrimaryKeyColumnWithMinValue("PkInt")
	startPK.AddPrimaryKeyColumnWithMinValue("PkBinary")
	endPK := new(tablestore.PrimaryKey)
	endPK.AddPrimaryKeyColumnWithMaxValue("PkString")
	endPK.AddPrimaryKeyColumnWithMaxValue("PkInt")
	endPK.AddPrimaryKeyColumnWithMaxValue("PkBinary")
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	rangeRowQueryCriteria.EndPrimaryKey = endPK
	rangeRowQueryCriteria.Direction = tablestore.FORWARD
	rangeRowQueryCriteria.MaxVersion = 1
	rangeRowQueryCriteria.Limit = 5000
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria
	return getRangeRequest
}

func getRangeRestoreTable(client tablestore.TableStoreApi, tableName string, c *C) []*tablestore.Row {
	rows := make([]*tablestore.Row, 0)
	getRangeReq := buildGetRangeReq(tableName)
	for {
		resp, err := client.GetRange(getRangeReq)
		c.Assert(err, IsNil)
		rows = append(rows, resp.Rows...)
		if resp.NextStartPrimaryKey != nil {
			getRangeReq.RangeRowQueryCriteria.StartPrimaryKey = resp.NextStartPrimaryKey
		} else {
			break
		}
	}
	return rows
}

func compareSameKeyRestoreRows(want []*tablestore.Row, got []*tablestore.Row, c *C) {
	c.Assert(len(want), Equals, len(got))
	for i, row := range got {
		equal := reflect.DeepEqual(row.PrimaryKey, want[i].PrimaryKey)
		c.Assert(equal, Equals, true)
		c.Assert(row.Columns[0].Value, Equals, int64(4999))
	}
}

func compareDiffKeyRestoreRows(want []*tablestore.Row, got []*tablestore.Row, c *C) {
	c.Assert(len(want), Equals, len(got))
	for i, row := range got {
		equal := reflect.DeepEqual(row.PrimaryKey, want[i].PrimaryKey)
		c.Assert(equal, Equals, true)
		c.Assert(row.Columns[0].Value, Equals, int64(i))
	}
}

func compareFuzzyRestoreRows(want []*tablestore.Row, got []*tablestore.Row, c *C) {
	c.Assert(len(want), Equals, len(got))
	for i, row := range got {
		equal := reflect.DeepEqual(row.PrimaryKey, want[i].PrimaryKey)
		c.Assert(equal, Equals, true)
		c.Assert(row.Columns[0].Value, Equals, int64(2*i))
	}
}

func compareRestoreRowsIncludeAutoInc(want []*tablestore.Row, got []*tablestore.Row, autoIncIndex int, c *C) {
	c.Assert(len(want), Equals, len(got))
	for i, row := range got {
		wantPk := want[i].PrimaryKey.PrimaryKeys
		wantCol := want[i].Columns
		for j, pk := range row.PrimaryKey.PrimaryKeys {
			if autoIncIndex == 0 || j != autoIncIndex {
				equal := reflect.DeepEqual(pk, wantPk[j])
				c.Assert(equal, Equals, true)
			}
		}
		for j, col := range row.Columns {
			c.Assert(col.ColumnName, Equals, wantCol[j].ColumnName)
			c.Assert(col.Value, Equals, wantCol[j].Value.(string))
		}
	}
}

func getExpectRowsForAutoIncWithoutRegen(col string) []*tablestore.Row {
	return []*tablestore.Row{
		{
			PrimaryKey: &tablestore.PrimaryKey{
				PrimaryKeys: []*tablestore.PrimaryKeyColumn{
					{
						ColumnName: "PkString",
						Value:      "pk1",
					},
					{
						ColumnName: "PkInt",
						Value:      int64(1),
					},
					{
						ColumnName: "PkBinary",
						Value:      []byte("pkBinary"),
					},
				},
			},
			Columns: []*tablestore.AttributeColumn{
				{
					ColumnName: col,
					Value:      "colVal1",
				},
			},
		},
		{
			PrimaryKey: &tablestore.PrimaryKey{
				PrimaryKeys: []*tablestore.PrimaryKeyColumn{
					{
						ColumnName: "PkString",
						Value:      "pk1",
					},
					{
						ColumnName: "PkInt",
						Value:      int64(2),
					},
					{
						ColumnName: "PkBinary",
						Value:      []byte("pkBinary"),
					},
				},
			},
			Columns: []*tablestore.AttributeColumn{
				{
					ColumnName: col,
					Value:      "colVal1",
				},
			},
		},
	}
}
