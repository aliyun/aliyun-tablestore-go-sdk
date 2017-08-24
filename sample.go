package main
import (
	"os"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/sample"
)

func main() {
	// Replace the endpoint info
	endpoint := os.Getenv("TS_TEST_ENDPOINT")
	instanceName := os.Getenv("TS_TEST_INSTANCENAME")
	accessKeyId := os.Getenv("TS_TEST_KEYID")
	accessKeySecret := os.Getenv("TS_TEST_SECRET")
	client := tablestore.NewClient(endpoint, instanceName, accessKeyId, accessKeySecret)

	// Table operation
	sample.CreateTableSample(client, "sampletable")
	sample.CreateTableKeyAutoIncrementSample(client)
	sample.DeleteTableSample(client)
	sample.ListTableSample(client)
	sample.UpdateTableSample(client, "sampletable")
	sample.DescribeTableSample(client, "sampletable")

	// Single row operation
	sample.PutRowSample(client,"sampletable")
	sample.UpdateRowSample(client, "sampletable")
	sample.GetRowSample(client, "sampletable")
	sample.DeleteRowSample(client, "sampletable")
	sample.PutRowWithKeyAutoIncrementSample(client)

	// Multi row operation
	sample.BatchWriteRowSample(client,"sampletable")
	sample.BatchGetRowSample(client, "sampletable")
	sample.GetRangeSample(client, "sampletable")

	// Stream sample
	sample.GetStreamRecordSample(client, "streamtable1")
}