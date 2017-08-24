package sample

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"time"
	"strconv"
)

func BatchWriteRowSample(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("batch write row started")
	batchWriteReq := &tablestore.BatchWriteRowRequest{}

	for i := 0; i < 100; i++ {
		putRowChange := new(tablestore.PutRowChange)
		putRowChange.TableName = tableName
		putPk := new(tablestore.PrimaryKey)
		putPk.AddPrimaryKeyColumn("pk1", "pk1value1")
		putPk.AddPrimaryKeyColumn("pk2", int64(i))
		putPk.AddPrimaryKeyColumn("pk3", []byte("pk3"))
		putRowChange.PrimaryKey = putPk
		putRowChange.AddColumn("col1", "fixvalue")
		putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		batchWriteReq.AddRowChange(putRowChange)
	}

	response, err := client.BatchWriteRow(batchWriteReq)
	if err != nil {
		fmt.Println("batch request failed with:", response)
	} else {
		// todo check all succeed
		fmt.Println("batch write row finished")
	}
}

func BatchGetRowSample(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("batch get row started")
	batchGetReq := &tablestore.BatchGetRowRequest{}
	mqCriteria := &tablestore.MultiRowQueryCriteria{}

	for i := 0; i < 20; i++ {
		pkToGet := new(tablestore.PrimaryKey)
		pkToGet.AddPrimaryKeyColumn("pk1", "pk1value1")
		pkToGet.AddPrimaryKeyColumn("pk2", int64(i))
		pkToGet.AddPrimaryKeyColumn("pk3", []byte("pk3"))
		mqCriteria.AddRow(pkToGet)
	}
	pkToGet2 := new(tablestore.PrimaryKey)
	pkToGet2.AddPrimaryKeyColumn("pk1", "pk1value2")
	pkToGet2.AddPrimaryKeyColumn("pk2", int64(300))
	pkToGet2.AddPrimaryKeyColumn("pk3", []byte("pk3"))
	mqCriteria.AddColumnToGet("col1")
	mqCriteria.AddRow(pkToGet2)

	mqCriteria.MaxVersion = 1
	mqCriteria.TableName = tableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)


	/*condition := tablestore.NewSingleColumnCondition("col1", tablestore.CT_GREATER_THAN, int64(0))
	mqCriteria.Filter = condition*/

	batchGetResponse, err := client.BatchGetRow(batchGetReq)

	if err != nil {
		fmt.Println("batachget failed with error:", err)
	} else {
		for _, row := range (batchGetResponse.TableToRowsResult[mqCriteria.TableName]) {
			if row.PrimaryKey.PrimaryKeys != nil {
				fmt.Println("get row with key", row.PrimaryKey.PrimaryKeys[0].Value, row.PrimaryKey.PrimaryKeys[1].Value, row.PrimaryKey.PrimaryKeys[2].Value)
			} else {
				fmt.Println("this row is not exist")
			}
		}
		fmt.Println("batchget finished")
	}
}

func GetRangeSample(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("Begin to scan the table")

	getRangeRequest := &tablestore.GetRangeRequest{}
	rangeRowQueryCriteria := &tablestore.RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = tableName

	startPK := new(tablestore.PrimaryKey)
	startPK.AddPrimaryKeyColumnWithMinValue("pk1")
	startPK.AddPrimaryKeyColumnWithMinValue("pk2")
	startPK.AddPrimaryKeyColumnWithMinValue("pk3")
	endPK := new(tablestore.PrimaryKey)
	endPK.AddPrimaryKeyColumnWithMaxValue("pk1")
	endPK.AddPrimaryKeyColumnWithMaxValue("pk2")
	endPK.AddPrimaryKeyColumnWithMaxValue("pk3")
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	rangeRowQueryCriteria.EndPrimaryKey = endPK
	rangeRowQueryCriteria.Direction = tablestore.FORWARD
	rangeRowQueryCriteria.MaxVersion = 1
	rangeRowQueryCriteria.Limit = 10
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria

	getRangeResp, err := client.GetRange(getRangeRequest)

	fmt.Println("get range result is " ,getRangeResp)

	for ; ; {
		if err != nil {
			fmt.Println("get range failed with error:", err)
		}
		if (len(getRangeResp.Rows) > 0) {
			for _, row := range getRangeResp.Rows {
				fmt.Println("range get row with key", row.PrimaryKey.PrimaryKeys[0].Value, row.PrimaryKey.PrimaryKeys[1].Value, row.PrimaryKey.PrimaryKeys[2].Value)
			}
			if getRangeResp.NextStartPrimaryKey == nil {
				break
			} else {
				fmt.Println("next pk is :", getRangeResp.NextStartPrimaryKey.PrimaryKeys[0].Value, getRangeResp.NextStartPrimaryKey.PrimaryKeys[1].Value, getRangeResp.NextStartPrimaryKey.PrimaryKeys[2].Value)
				getRangeRequest.RangeRowQueryCriteria.StartPrimaryKey = getRangeResp.NextStartPrimaryKey
				getRangeResp, err = client.GetRange(getRangeRequest)
			}
		} else {
			break
		}

		fmt.Println("continue to query rows")
	}
	fmt.Println("putrow finished")

}

func GetStreamRecordSample(client *tablestore.TableStoreClient, tableName string) {
	createtableRequest := new(tablestore.CreateTableRequest)

	client.DeleteTable(&tablestore.DeleteTableRequest{TableName: tableName})

	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk2", tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk3", tablestore.PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk4", tablestore.PrimaryKeyType_INTEGER)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput
	createtableRequest.StreamSpec = &tablestore.StreamSpecification{EnableStream: true, ExpirationTime: 24}

	_, err := client.CreateTable(createtableRequest)
	if (err != nil) {
		fmt.Println("Failed to create table with error:", err)
	} else {
		fmt.Println("Create table finished")
	}


	time.Sleep(time.Millisecond * 20)

	for i := 0; i < 20 ; i++ {
		req := tablestore.PutRowRequest{}
		rowChange := tablestore.PutRowChange{}
		rowChange.TableName = tableName
		pk := tablestore.PrimaryKey{}
		pk.AddPrimaryKeyColumn("pk1", "01f3")
		pk.AddPrimaryKeyColumn("pk2", "000001")
		pk.AddPrimaryKeyColumn("pk3", "001")
		val := 1495246210 + i * 100
		pk.AddPrimaryKeyColumn("pk4", int64(val))

		rowChange.PrimaryKey = &pk

		val1 := float64(120.1516525097) + float64(0.0000000001) * float64(i)

		rowChange.AddColumn("longitude", strconv.FormatFloat(val1, 'g', 1, 64))
		rowChange.AddColumn("latitude", "30.2583277934")
		rowChange.AddColumn("brand", "BMW")

		rowChange.AddColumn("speed", "25")
		rowChange.AddColumn("wind_speed", "2")
		rowChange.AddColumn("temperature", "20")
		distance := 8000 + i;
		rowChange.AddColumn("distance",  strconv.Itoa(distance))

		rowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		req.PutRowChange = &rowChange
		_, err = client.PutRow(&req)

		if err != nil {
			fmt.Print(err)
		}
	}

	resp, err := client.ListStream(&tablestore.ListStreamRequest{TableName: &tableName})

	fmt.Printf("%#v\n", resp)

	streamId := resp.Streams[0].Id

	resp2, err := client.DescribeStream(&tablestore.DescribeStreamRequest{StreamId: streamId})
	fmt.Printf("DescribeStreamResponse: %#v\n", resp)
	fmt.Printf("StreamShard: %#v\n", resp2.Shards[0])
	shardId := resp2.Shards[0].SelfShard

	resp3, err := client.GetShardIterator(&tablestore.GetShardIteratorRequest{
		StreamId: streamId,
		ShardId: shardId})

	iter := resp3.ShardIterator

	records := make([]*tablestore.StreamRecord, 0)
	for {
		resp, err := client.GetStreamRecord(&tablestore.GetStreamRecordRequest{
			ShardIterator: iter})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("#records: %d\n", len(resp.Records))
		for i, rec := range resp.Records {
			fmt.Printf("record %d: %s\n", i, rec)
		}
		for _, rec := range resp.Records {
			records = append(records, rec)
		}
		nextIter := resp.NextShardIterator
		if nextIter == nil {
			fmt.Printf("next iterator: %#v\n", nextIter)
			break
		} else {
			fmt.Printf("next iterator: %#v\n", *nextIter)
		}
		if *iter == *nextIter {
			break
		}
		iter = nextIter
	}

}