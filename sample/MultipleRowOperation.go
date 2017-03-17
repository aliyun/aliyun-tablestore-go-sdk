package sample

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
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

	for i := 0; i < 100; i++ {
		pkToGet := new(tablestore.PrimaryKey)
		pkToGet.AddPrimaryKeyColumn("pk1", "pk1value1")
		pkToGet.AddPrimaryKeyColumn("pk2", int64(i))
		pkToGet.AddPrimaryKeyColumn("pk3", []byte("pk3"))
		mqCriteria.AddRow(pkToGet)
		mqCriteria.MaxVersion = 1
	}

	mqCriteria.TableName = tableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, err := client.BatchGetRow(batchGetReq)

	if err != nil {
		fmt.Println("putrow failed with error:", err)
	} else {
		for _, row := range (batchGetResponse.TableToRowsResult[mqCriteria.TableName]) {
			fmt.Println("get row with key", row.PrimaryKey.PrimaryKeys[0].Value, row.PrimaryKey.PrimaryKeys[1].Value, row.PrimaryKey.PrimaryKeys[2].Value)
		}
		fmt.Println("putrow finished")
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
				fmt.Println("len is",len(getRangeResp.NextStartPrimaryKey.PrimaryKeys))
				fmt.Println("next pk is :", getRangeResp.NextStartPrimaryKey.PrimaryKeys[0].ColumnName)
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