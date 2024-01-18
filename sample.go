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

	// SearchQuery
	sample.CreateSearchIndexForSearchQuery(client, "query_sample_table", "query_sample_index")
	sample.WriteDateForSearchQuery(client, "query_sample_table")
	sample.SearchQuery(client, "query_sample_table", "query_sample_index")

	//SearchIndex: agg & group by
	sample.CreateSearchIndexForAggregationAndGroupBy(client, "agg_sample_table", "agg_sample_index")
	sample.WriteDataForAggregationAndGroupBy(client, "agg_sample_table")
	sample.AggregationSample(client, "agg_sample_table", "agg_sample_index")
	sample.AvgAggregationSample(client, "agg_sample_table", "agg_sample_index")
	sample.DistinctAggregationSample(client, "agg_sample_table", "agg_sample_index")
	sample.MaxAggregationSample(client, "agg_sample_table", "agg_sample_index")
	sample.SumAggregationSample(client, "agg_sample_table", "agg_sample_index")
	sample.CountAggregationSample(client, "agg_sample_table", "agg_sample_index")
	sample.TopRowsAggregationSample(client, "agg_sample_table", "agg_sample_index")
	sample.PercentilesAggregationSample(client, "agg_sample_table", "agg_sample_index")

	sample.GroupBySample(client, "agg_sample_table", "agg_sample_index")
	sample.GroupByFieldSample(client, "agg_sample_table", "agg_sample_index")
	sample.GroupByRangeSample(client, "agg_sample_table", "agg_sample_index")
	sample.GroupByFilterSample(client, "agg_sample_table", "agg_sample_index")
	sample.GroupByGeoDistanceSample(client, "agg_sample_table", "agg_sample_index")
	sample.GroupByHistogramSample(client, "agg_sample_table", "agg_sample_index")
	sample.GroupByDateHistogramSample(client, "agg_sample_table", "agg_sample_index")
	sample.GroupByGeoGridSample(client, "agg_sample_table", "agg_sample_index")

	sample.ParallelScanSingleConcurrency(client, "scan_sample_table", "scan_sample_index")
	sample.ParallelScanMultiConcurrency(client, "scan_sample_table", "scan_sample_index")

	// SearchIndex: highlighting
	sample.CreateSearchIndexForQueryHighlighting(client, "highlighting_sample_table", "highlighting_sample_index")
	sample.WriteDataForQueryHighlighting(client, "highlighting_sample_table")
	sample.QueryHighlightingSample(client, "highlighting_sample_table", "highlighting_sample_index")

	// SearchIndex: Vector Query
	sample.CreateSearchIndexForVectorQuery(client, "vector_query_sample_table", "vector_query_sample_index")
	sample.WriteDataForVectorQuery(client, "vector_query_sample_table")
	sample.VectorQuerySample(client, "vector_query_sample_table", "vector_query_sample_index")

	// update searchIndex schema
	sample.UpdateSearchIndexSchema(client, "go_sdk_test_table", "go_sdk_test_index", "go_sdk_test_index_reindex")

	// SQL sample
	sample.SQLQuerySample(client)

	// Server side encryption sample
	sample.ServerSideEncryptionSample(client)
}
