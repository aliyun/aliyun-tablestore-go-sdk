package sample

import (
	"fmt"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

/**
CreateTimeseriesTableSample 创建一个时序表，其中表名为：timeseriesTableName，TTL为：timetolive。
 */
func CreateTimeseriesTableSample(client *tablestore.TimeseriesClient, timeseriesTableName string , timetoLive int64) {
	fmt.Println("[Info]: Begin to create timeseries table: " , timeseriesTableName)

	timeseriesTableOptions := tablestore.NewTimeseriesTableOptions(timetoLive)		// 构造表选项

	// 构造表元数据信息
	timeseriesTableMeta := tablestore.NewTimeseriesTableMeta(timeseriesTableName)	// 设置表名
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)			// 设置表选项

	createTimeseriesTableRequest := tablestore.NewCreateTimeseriesTableRequest()	// 构造创建时序表请求
	createTimeseriesTableRequest.SetTimeseriesTableMeta(timeseriesTableMeta)

	createTimeseriesTableResponse , err := client.CreateTimeseriesTable(createTimeseriesTableRequest)		// 调用client创建时序表
	if err != nil {
		fmt.Println("[Error]: Failed to create timeseries table with error: " , err)
		return
	}
	fmt.Println("[Info]: CreateTimeseriesTable finished ! RequestId: " , createTimeseriesTableResponse.RequestId)
}

/**
* DescribeTimeseriesTableSample 获取时序表timeseriesTableName的元数据信息。
 */
func DescribeTimeseriesTableSample(client *tablestore.TimeseriesClient , timeseriesTableName string) {
	fmt.Println("[Info]: Begin to require timeseries table description ！")
	describeTimeseriesTableRequest := tablestore.NewDescribeTimeseriesTableRequset(timeseriesTableName)		// 构造请求，并设置请求表名

	describeTimeseriesTableResponse , err := client.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Failed to require timeseries table description !")
		return
	}
	fmt.Println("[Info]: DescribeTimeseriesTableSample finished. Timeseries table meta: ")
	fmt.Println("	[Info]: TimeseriesTableName: " , describeTimeseriesTableResponse.GetTimeseriesTableMeta().GetTimeseriesTableName())
	fmt.Println("	[Info]: TimeseriesTable TTL: " , describeTimeseriesTableResponse.GetTimeseriesTableMeta().GetTimeseriesTableOPtions().GetTimeToLive())
}

/**
* ListTimeseriesTableSample 列出实例中所有时序表的元数据信息
 */
func ListTimeseriesTableSample(client *tablestore.TimeseriesClient) {
	fmt.Println("[Info]: Begin to list timeseries table !")
	listTimeseriesTableResponse , err := client.ListTimeseriesTable()
	if err != nil {
		fmt.Println("[Info]: List timeseries table failed with error: " , err)
	}
	fmt.Println("[Info]: Timeseries table Meta: ")
	for i := 0; i < len(listTimeseriesTableResponse.GetTimeseriesTableMeta()); i++ {
		curTimeseriesTableMeta := listTimeseriesTableResponse.GetTimeseriesTableMeta()[i]
		fmt.Println("	[Info]: Timeseries table name: " , curTimeseriesTableMeta.GetTimeseriesTableName() , " TTL: " , curTimeseriesTableMeta.GetTimeseriesTableOPtions().GetTimeToLive())
	}
	fmt.Println("[Info]: ListTimeseriesTableSample finished !")
}

/**
DeleteTimeseriesTableSample 删除实例中表名为timeseriesTableName的时序表
 */
func DeleteTimeseriesTableSample(client *tablestore.TimeseriesClient , timeseriesTableName string) {
	fmt.Println("[Info]: Begin to delete timeseries table !")
	// 构造删除时序表请求
	deleteTimeseriesTableRequest := tablestore.NewDeleteTimeseriesTableRequest(timeseriesTableName)
	// 调用时序客户端删除时序表
	deleteTimeseriesTableResponse , err := client.DeleteTimeseriesTable(deleteTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Delete timeseries table failed with error: " , err)
		return
	}
	fmt.Println("[Info]: DeleteTimeseriesTableSample finished ! RequestId: " , deleteTimeseriesTableResponse.RequestId)
}

/**
* UpdateTimeseriesTableSample 更新时序表的TTL参数
 */
func UpdateTimeseriesTableSample(client *tablestore.TimeseriesClient , timeseriesTableName string) {
	fmt.Println("[Info]: Begin to update timeseries table !")
	// 构造时序表TTL参数选项
	timeseriesTableOptions := tablestore.NewTimeseriesTableOptions(964000)

	// 构造更新请求
	updateTimeseriesTableRequest := tablestore.NewUpdateTimeseriesTableRequest(timeseriesTableName)
	updateTimeseriesTableRequest.SetTimeseriesTableOptions(timeseriesTableOptions)

	// 调用时序客户端更新时序表
	updateTimeseriesTableResponse , err := client.UpdateTimeseriesTable(updateTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Update timeseries table failed with error: " , err)
		return
	}
	DescribeTimeseriesTableSample(client , timeseriesTableName)
	fmt.Println("[Info]: UpdateTimeseriesTableSample finished ! RequestId: " , updateTimeseriesTableResponse.RequestId)
}

/**
* PutTimeseriesDataSample 向时序表中写入一个或多个时序数据。
 */
func PutTimeseriesDataSample(client *tablestore.TimeseriesClient , timeseriesTableName string) {
	fmt.Println("[Info]: Begin to PutTimeseriesDataSample !")

	// 构造时序数据行timeseriesRow
	timeseriesKey := tablestore.NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("CPU")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City" , "Hangzhou")
	timeseriesKey.AddTag("Region" , "Xihu")

	timeseriesRow := tablestore.NewTimeseriesRow(timeseriesKey)
	timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000)
	timeseriesRow.AddField("temperature" , tablestore.NewColumnValue(tablestore.ColumnType_INTEGER , 98))
	timeseriesRow.AddField("status" , tablestore.NewColumnValue(tablestore.ColumnType_STRING , "ok"))

	// 构造时序数据行timeseriesRow1
	timeseriesKey1 := tablestore.NewTimeseriesKey()
	timeseriesKey1.SetMeasurementName("NETWORK")
	timeseriesKey1.SetDataSource("127.0.0.1")
	timeseriesKey1.AddTag("City" , "Hangzhou")
	timeseriesKey1.AddTag("Region" , "Xihu")

	timeseriesRow1 := tablestore.NewTimeseriesRow(timeseriesKey1)
	timeseriesRow1.SetTimeInus(time.Now().UnixNano() / 1000)
	timeseriesRow1.AddField("in" , tablestore.NewColumnValue(tablestore.ColumnType_INTEGER , 1000))
	timeseriesRow1.AddField("data" , tablestore.NewColumnValue(tablestore.ColumnType_BINARY , []byte("tablestore")))
	timeseriesRow1.AddField("program" , tablestore.NewColumnValue(tablestore.ColumnType_STRING , "tablestore.d"))
	timeseriesRow1.AddField("status" , tablestore.NewColumnValue(tablestore.ColumnType_BOOLEAN, true))
	timeseriesRow1.AddField("lossrate" , tablestore.NewColumnValue(tablestore.ColumnType_DOUBLE , float64(1.9098)))

	// 构造put时序数据请求
	putTimeseriesDataRequest := tablestore.NewPutTimeseriesDataRequest(timeseriesTableName)
	putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow , timeseriesRow1)

	// 调用时序客户端写入时序数据
	putTimeseriesDataResponse , err := client.PutTimeseriesData(putTimeseriesDataRequest)
	if err != nil {
		fmt.Println("[Error]: Put timeseries data Failed with error: " , err)
		return
	}
	if len(putTimeseriesDataResponse.GetFailedRowResults()) > 0 {
		fmt.Println("[Warning]: Put timeseries data finished ! Some of timeseries row put Failed: ")
		for i := 0; i < len(putTimeseriesDataResponse.GetFailedRowResults()); i++ {
			FailedRow := putTimeseriesDataResponse.GetFailedRowResults()[i]
			fmt.Println("	[Warning]: Failed Row: Index: " , FailedRow.Index , " Error: " , FailedRow.Error)
		}
	} else {
		fmt.Println("[Info]: PutTimeseriesDataSample finished ! RequestId: " , putTimeseriesDataResponse.RequestId)
	}
}

/**
* GetTimeseriesDataSample 根据timeseriesKey获取时序表中指定的时间线数据
 */
func GetTimeseriesDataSample(client *tablestore.TimeseriesClient , timeseriesTableName string) {
	fmt.Println("[Info]: Begin to get timeseries data !")

	// 构造待查询时间线的timeseriesKey
	timeseriesKey := tablestore.NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("NETWORK")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City" , "Hangzhou")
	timeseriesKey.AddTag("Region" , "Xihu")

	// 构造get请求
	getTimeseriesDataRequest := tablestore.NewGetTimeseriesDataRequest(timeseriesTableName)
	getTimeseriesDataRequest.SetTimeseriesKey(timeseriesKey)
	getTimeseriesDataRequest.SetTimeRange(0 , time.Now().UnixNano() / 1000) 	// 指定查询时间线的范围
	getTimeseriesDataRequest.SetLimit(-1)

	// 调用时序客户端接口获取时间线数据
	getTimeseriesResp , err := client.GetTimeseriesData(getTimeseriesDataRequest)
	if err != nil {
		fmt.Println("[Error]: Get timeseries data Failed with error: " , err)
		return
	}
	fmt.Println("[Info]: Get timeseries data succeed ! TimeseriesRows: ")
	for i := 0; i < len(getTimeseriesResp.GetRows()); i++ {
		fmt.Println("	[Info]: Row" , i , ": [" , getTimeseriesResp.GetRows()[i].GetTimeseriesKey().GetMeasurementName() ,
			getTimeseriesResp.GetRows()[i].GetTimeseriesKey().GetDataSource(),
			getTimeseriesResp.GetRows()[i].GetTimeseriesKey().GetTags(), "]",
			getTimeseriesResp.GetRows()[i].GetFieldsSlice() ,
			getTimeseriesResp.GetRows()[i].GetTimeInus())
	}
	fmt.Println("[Info]: GetTimeseriesDataSample finished! RequestId: " , getTimeseriesResp.RequestId)
}

/**
* QueryTimeseriesMetaSample 根据指定条件查询数据表中特定时间线的measurement、source、tag信息，其中查询条件可组合。
 */
func QueryTimeseriesMetaSample(client *tablestore.TimeseriesClient , timeseriesTableName string) {
	fmt.Println("[Info]: Begin to query timeseries table meta !")

	// 构造多个单查询条件
	measurementMetaQueryCondition := tablestore.NewMeasurementQueryCondition(tablestore.OP_GREATER_EQUAL , "")
	datasourceMetaQueryCondition := tablestore.NewDataSourceMetaQueryCondition(tablestore.OP_GREATER_EQUAL , "")
	tagMetaQueryCondition := tablestore.NewTagMetaQueryCondition(tablestore.OP_GREATER_THAN , "City" , "")

	// 构造组合条件
	compsiteMetaQueryCondition := tablestore.NewCompositeMetaQueryCondition(tablestore.OP_AND)
	compsiteMetaQueryCondition.AddSubConditions(measurementMetaQueryCondition)
	compsiteMetaQueryCondition.AddSubConditions(datasourceMetaQueryCondition)
	compsiteMetaQueryCondition.AddSubConditions(tagMetaQueryCondition)

	// 构造query请求
	queryTimeseriesMetaRequest := tablestore.NewQueryTimeseriesMetaRequest(timeseriesTableName)
	queryTimeseriesMetaRequest.SetCondition(compsiteMetaQueryCondition)
	queryTimeseriesMetaRequest.SetLimit(-1)

	// 调用客户端执行查询请求
	queryTimeseriesTableResponse , err := client.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	if err != nil {
		fmt.Println("[Error]: Query timeseries table meta failed with error: " , err)
		return
	}
	fmt.Println("	[Info]: Query timeseries table meta succeed: ")
	for i := 0; i < len(queryTimeseriesTableResponse.GetTimeseriesMetas()); i++ {
		curTimeseriesMeta := queryTimeseriesTableResponse.GetTimeseriesMetas()[i]
		fmt.Println("	[Info]: Meta_" , i , ": " , "Measurement: " , curTimeseriesMeta.GetTimeseriesKey().GetMeasurementName() ,
			"Source: " , curTimeseriesMeta.GetTimeseriesKey().GetDataSource() ,
			"Tags: " , curTimeseriesMeta.GetTimeseriesKey().GetTags() ,
			"Attrs: " , curTimeseriesMeta.GetAttributeSlice())
	}
	fmt.Println("[Info]: QueryTimeseriesMetaSample finished !")
}

/**
* UpdateTimeseriesMetaSample 更新时间线中的Attributes信息。
 */
func UpdateTimeseriesMetaSample(tsClient *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to update timeseries meta !")

	PutTimeseriesDataSample(tsClient , timeseriesTableName)

	updateTimeseriesMetaRequest := tablestore.NewUpdateTimeseriesMetaRequest(timeseriesTableName)

	timeseriesKey := tablestore.NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("NETWORK")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City" , "Hangzhou")
	timeseriesKey.AddTag("Region" , "Xihu")

	timeseriesMeta := tablestore.NewTimeseriesMeta(timeseriesKey)
	//timeseriesMeta.SetUpdateTimeInUs(96400)
	timeseriesMeta.AddAttribute("NewRegion" , "Yuhang")
	timeseriesMeta.AddAttribute("NewCity" , "Shanghai")

	updateTimeseriesMetaRequest.AddTimeseriesMetas(timeseriesMeta)

	updateTimeseriesMetaResponse , err := tsClient.UpdateTimeseriesMeta(updateTimeseriesMetaRequest)
	if err != nil {
		fmt.Println("[Error]: Update timeseries meta failed with error: " , err)
		return
	}

	if len(updateTimeseriesMetaResponse.GetFailedRowResults()) > 0 {
		fmt.Println("	[Error]: Update timeseries meta failed row: ")
		for i := 0; i < len(updateTimeseriesMetaResponse.GetFailedRowResults()); i++ {
			fmt.Println("	[Error]: " , updateTimeseriesMetaResponse.GetFailedRowResults()[i].Index , updateTimeseriesMetaResponse.GetFailedRowResults()[i].Error)
		}
	}

	QueryTimeseriesMetaSample(tsClient , timeseriesTableName)

	fmt.Println("[Info]: UpdateTimeseriesMetaSample finished !")
}





