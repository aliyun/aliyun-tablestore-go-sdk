package main

import (
	"os"

	"github.com/aliyun/aliyun-tablestore-go-sdk/sample"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

func main() {
	// Replace the endpoint info
	endpoint := os.Getenv("OTS_TEST_ENDPOINT")
	instanceName := os.Getenv("OTS_TEST_INSTANCENAME")
	accessKeyId := os.Getenv("OTS_TEST_KEYID")
	accessKeySecret := os.Getenv("OTS_TEST_SECRET")
	client := tablestore.NewClient(endpoint, instanceName, accessKeyId, accessKeySecret)

	sample.UpdateRowWithIncrement(client, "sampletable")
	//return
	// Table operation
	sample.CreateTableSample(client, "sampletable")
	sample.CreateTableKeyAutoIncrementSample(client)
	sample.DeleteTableSample(client)
	sample.ListTableSample(client)
	sample.UpdateTableSample(client, "sampletable")
	sample.DescribeTableSample(client, "sampletable")

	// Single row operation
	sample.PutRowSample(client, "sampletable")
	sample.UpdateRowSample(client, "sampletable")
	sample.GetRowSample(client, "sampletable")
	sample.DeleteRowSample(client, "sampletable")
	sample.PutRowWithKeyAutoIncrementSample(client)

	// Multi row operation
	sample.BatchWriteRowSample(client, "sampletable")
	sample.BatchGetRowSample(client, "sampletable")
	sample.GetRangeSample(client, "sampletable")

	// Stream sample
	// sample.GetStreamRecordSample(client, "streamtable1")

	// computeSplitpoint
	sample.ComputeSplitPointsBySize(client, "sampletable")

	// transaction
	sample.PutRowWithTxnSample(client, "transtable1")

	// globalindex
	sample.CreateTableWithGlobalIndexSample(client, "globalindex1")

	//SearchIndex
	sample.CreateSearchIndexWithVirtualField(client, "virtual_sample_table", "virtual_sample_index")

	//SearchIndex: agg & group by
	sample.CreateSearchIndexForAggregationAndGroupBy(client, "agg_sample_table", "agg_sample_index")
	sample.WriteDataForAggregationAndGroupBy(client, "agg_sample_table")
	sample.AggregationSample(client, "agg_sample_table", "agg_sample_index")
	sample.GroupBySample(client, "agg_sample_table", "agg_sample_index")

	sample.ParallelScanSingleConcurrency(client, "scan_sample_table", "scan_sample_index")
	sample.ParallelScanMultiConcurrency(client, "scan_sample_table", "scan_sample_index")
}
