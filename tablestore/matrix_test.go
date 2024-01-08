package tablestore

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"os"
	"testing"
)

var (
	matrixData  []byte
	plainBuffer []byte
)

func init() {
	fmt.Println("begin init")
	endpoint := os.Getenv("OTS_TEST_ENDPOINT")
	instanceName := os.Getenv("OTS_TEST_INSTANCENAME")
	accessKeyId := os.Getenv("OTS_TEST_KEYID")
	accessKeySecret := os.Getenv("OTS_TEST_SECRET")
	// init the global client
	client = NewClient(endpoint, instanceName, accessKeyId, accessKeySecret)

	var benchTable = "tablestoreBenchMark"
	err := PrepareFuzzyTable(benchTable)
	if err != nil {
		panic(err)
	}
	_, err = PrepareFuzzyTableData(benchTable, 1024, 10240, 500)
	if err != nil {
		panic(err)
	}
	plainBuffer, err = rangeFuzzyTableBuf(benchTable, PlainBuffer, 5000, fuzzyMetaAttr)
	if err != nil {
		panic(err)
	}
	matrixData, err = rangeFuzzyTableBuf(benchTable, SimpleRowMatrix, 5000, fuzzyMetaAttr)
	if err != nil {
		panic(err)
	}
	//GetRangeSample(client.(*TableStoreClient), benchTable)
	fmt.Println("begin done")
}

func rangeFuzzyTableBuf(table string, blockType DataBlockType, count int32, cols []string) ([]byte, error) {
	startPk := new(PrimaryKey)
	startPk.AddPrimaryKeyColumnWithMinValue(fuzzyMetaPk1)
	startPk.AddPrimaryKeyColumnWithMinValue(fuzzyMetaPk2)
	startPk.AddPrimaryKeyColumnWithMinValue(fuzzyMetaPk3)
	endPk := new(PrimaryKey)
	endPk.AddPrimaryKeyColumnWithMaxValue(fuzzyMetaPk1)
	endPk.AddPrimaryKeyColumnWithMaxValue(fuzzyMetaPk2)
	endPk.AddPrimaryKeyColumnWithMaxValue(fuzzyMetaPk3)

	req := &otsprotocol.GetRangeRequest{
		TableName:                proto.String(table),
		Direction:                otsprotocol.Direction_FORWARD.Enum(),
		MaxVersions:              proto.Int32(1),
		Limit:                    proto.Int32(count),
		ColumnsToGet:             cols,
		InclusiveStartPrimaryKey: startPk.Build(false),
		ExclusiveEndPrimaryKey:   endPk.Build(false),
		DataBlockTypeHint:        toPBDataBlockType(blockType),
		CompressTypeHint:         toPBCompressType(None),
	}

	resp := new(otsprotocol.GetRangeResponse)
	response := &GetRangeResponse{ConsumedCapacityUnit: &ConsumedCapacityUnit{}}
	if err := client.(*TableStoreClient).doRequestWithRetry(getRangeUri, req, resp, &response.ResponseInfo, ExtraRequestInfo{}); err != nil {
		return nil, err
	}
	//if len(resp.NextStartPrimaryKey) != 0 {
	//	return nil, errors.New("scan trunc")
	//}
	return resp.Rows, nil
}

func BenchmarkParseMatrixRows(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := parseMatrixRows(matrixData)
		if err != nil {
			b.Fatal(err)
		}
		if i == 0 {
			fmt.Println("row count:", len(rows))
		}

	}
}

func BenchmarkPlainBuffer(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := parsePlainBufferRows(plainBuffer)
		if err != nil {
			b.Fatal(err)
		}
		if i == 0 {
			fmt.Println("row count:", len(rows))
		}
	}
}
