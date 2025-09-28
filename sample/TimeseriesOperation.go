package sample

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/golang/protobuf/proto"
)

/*
CreateTimeseriesTableSample creates a time series table, where the table name is: timeseriesTableName, and the TTL is: timetolive.
*/
func CreateTimeseriesTableSample(client *tablestore.TimeseriesClient, timeseriesTableName string, timetoLive int64) {
	fmt.Println("[Info]: Begin to create timeseries table: ", timeseriesTableName)

	timeseriesTableOptions := tablestore.NewTimeseriesTableOptions(timetoLive) // Construct table options

	// Construct table metadata information
	timeseriesTableMeta := tablestore.NewTimeseriesTableMeta(timeseriesTableName) // Set the table name
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)         // Set table options

	createTimeseriesTableRequest := tablestore.NewCreateTimeseriesTableRequest() // Construct a request to create a time-series table.
	createTimeseriesTableRequest.SetTimeseriesTableMeta(timeseriesTableMeta)

	createTimeseriesTableResponse, err := client.CreateTimeseriesTable(createTimeseriesTableRequest) // Call the client to create a time-series table
	if err != nil {
		fmt.Println("[Error]: Failed to create timeseries table with error: ", err)
		return
	}
	fmt.Println("[Info]: CreateTimeseriesTable finished ! RequestId: ", createTimeseriesTableResponse.RequestId)
}

/*
DescribeTimeseriesTableSample gets the metadata information of the timeseries table timeseriesTableName.
*/
func DescribeTimeseriesTableSample(client *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to require timeseries table description ！")
	describeTimeseriesTableRequest := tablestore.NewDescribeTimeseriesTableRequset(timeseriesTableName) // Construct the request and set the table name for the request.

	describeTimeseriesTableResponse, err := client.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Failed to require timeseries table description !")
		return
	}
	fmt.Println("[Info]: DescribeTimeseriesTableSample finished. Timeseries table meta: ")
	fmt.Println("	[Info]: TimeseriesTableName: ", describeTimeseriesTableResponse.GetTimeseriesTableMeta().GetTimeseriesTableName())
	fmt.Println("	[Info]: TimeseriesTable TTL: ", describeTimeseriesTableResponse.GetTimeseriesTableMeta().GetTimeseriesTableOPtions().GetTimeToLive())
}

/**
* ListTimeseriesTableSample lists the metadata information of all timeseries tables in the instance.
 */
func ListTimeseriesTableSample(client *tablestore.TimeseriesClient) {
	fmt.Println("[Info]: Begin to list timeseries table !")
	listTimeseriesTableResponse, err := client.ListTimeseriesTable()
	if err != nil {
		fmt.Println("[Info]: List timeseries table failed with error: ", err)
	}
	fmt.Println("[Info]: Timeseries table Meta: ")
	for i := 0; i < len(listTimeseriesTableResponse.GetTimeseriesTableMeta()); i++ {
		curTimeseriesTableMeta := listTimeseriesTableResponse.GetTimeseriesTableMeta()[i]
		fmt.Println("	[Info]: Timeseries table name: ", curTimeseriesTableMeta.GetTimeseriesTableName(), " TTL: ", curTimeseriesTableMeta.GetTimeseriesTableOPtions().GetTimeToLive())
	}
	fmt.Println("[Info]: ListTimeseriesTableSample finished !")
}

/*
DeleteTimeseriesTableSample deletes the timeseries table with the name 'timeseriesTableName' in the instance.
*/
func DeleteTimeseriesTableSample(client *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to delete timeseries table !")
	// Construct a delete request for the time-series table
	deleteTimeseriesTableRequest := tablestore.NewDeleteTimeseriesTableRequest(timeseriesTableName)
	// Call the time series client to delete a time series table
	deleteTimeseriesTableResponse, err := client.DeleteTimeseriesTable(deleteTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Delete timeseries table failed with error: ", err)
		return
	}
	fmt.Println("[Info]: DeleteTimeseriesTableSample finished ! RequestId: ", deleteTimeseriesTableResponse.RequestId)
}

/**
* UpdateTimeseriesTableSample updates the TTL parameter of a timeseries table
 */
func UpdateTimeseriesTableSample(client *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to update timeseries table !")
	// Construct the TTL parameter options for the time-series table.
	timeseriesTableOptions := tablestore.NewTimeseriesTableOptions(964000)

	// Construct an update request
	updateTimeseriesTableRequest := tablestore.NewUpdateTimeseriesTableRequest(timeseriesTableName)
	updateTimeseriesTableRequest.SetTimeseriesTableOptions(timeseriesTableOptions)

	// Call the timeline client to update the timeline table
	updateTimeseriesTableResponse, err := client.UpdateTimeseriesTable(updateTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Update timeseries table failed with error: ", err)
		return
	}
	DescribeTimeseriesTableSample(client, timeseriesTableName)
	fmt.Println("[Info]: UpdateTimeseriesTableSample finished ! RequestId: ", updateTimeseriesTableResponse.RequestId)
}

/**
* PutTimeseriesDataSample writes one or more time series data entries into a time series table.
 */
func PutTimeseriesDataSample(client *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to PutTimeseriesDataSample !")

	// Construct the timeseries data row `timeseriesRow`
	timeseriesKey := tablestore.NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("CPU")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City", "Hangzhou")
	timeseriesKey.AddTag("Region", "Xihu")

	timeseriesRow := tablestore.NewTimeseriesRow(timeseriesKey)
	timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000)
	timeseriesRow.AddField("temperature", tablestore.NewColumnValue(tablestore.ColumnType_INTEGER, 98))
	timeseriesRow.AddField("status", tablestore.NewColumnValue(tablestore.ColumnType_STRING, "ok"))

	// Construct the timeseries data row timeseriesRow1
	timeseriesKey1 := tablestore.NewTimeseriesKey()
	timeseriesKey1.SetMeasurementName("NETWORK")
	timeseriesKey1.SetDataSource("127.0.0.1")
	timeseriesKey1.AddTag("City", "Hangzhou")
	timeseriesKey1.AddTag("Region", "Xihu")

	timeseriesRow1 := tablestore.NewTimeseriesRow(timeseriesKey1)
	timeseriesRow1.SetTimeInus(time.Now().UnixNano() / 1000)
	timeseriesRow1.AddField("in", tablestore.NewColumnValue(tablestore.ColumnType_INTEGER, 1000))
	timeseriesRow1.AddField("data", tablestore.NewColumnValue(tablestore.ColumnType_BINARY, []byte("tablestore")))
	timeseriesRow1.AddField("program", tablestore.NewColumnValue(tablestore.ColumnType_STRING, "tablestore.d"))
	timeseriesRow1.AddField("status", tablestore.NewColumnValue(tablestore.ColumnType_BOOLEAN, true))
	timeseriesRow1.AddField("lossrate", tablestore.NewColumnValue(tablestore.ColumnType_DOUBLE, float64(1.9098)))

	// Construct the put sequential data request
	putTimeseriesDataRequest := tablestore.NewPutTimeseriesDataRequest(timeseriesTableName)
	putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow, timeseriesRow1)

	// Call the time series client to write time series data
	putTimeseriesDataResponse, err := client.PutTimeseriesData(putTimeseriesDataRequest)
	if err != nil {
		fmt.Println("[Error]: Put timeseries data Failed with error: ", err)
		return
	}
	if len(putTimeseriesDataResponse.GetFailedRowResults()) > 0 {
		fmt.Println("[Warning]: Put timeseries data finished ! Some of timeseries row put Failed: ")
		for i := 0; i < len(putTimeseriesDataResponse.GetFailedRowResults()); i++ {
			FailedRow := putTimeseriesDataResponse.GetFailedRowResults()[i]
			fmt.Println("	[Warning]: Failed Row: Index: ", FailedRow.Index, " Error: ", FailedRow.Error)
		}
	} else {
		fmt.Println("[Info]: PutTimeseriesDataSample finished ! RequestId: ", putTimeseriesDataResponse.RequestId)
	}
}

/**
* GetTimeseriesDataSample retrieves the specified timeline data from the time series table based on the timeseriesKey.
 */
func GetTimeseriesDataSample(client *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to get timeseries data !")

	// Construct the timeseriesKey for the timeline to be queried.
	timeseriesKey := tablestore.NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("NETWORK")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City", "Hangzhou")
	timeseriesKey.AddTag("Region", "Xihu")

	// Construct a GET request
	getTimeseriesDataRequest := tablestore.NewGetTimeseriesDataRequest(timeseriesTableName)
	getTimeseriesDataRequest.SetTimeseriesKey(timeseriesKey)
	getTimeseriesDataRequest.SetTimeRange(0, time.Now().UnixNano()/1000) // Specify the range of the query timeline
	getTimeseriesDataRequest.SetLimit(-1)

	// Call the timeline client interface to obtain timeline data.
	getTimeseriesResp, err := client.GetTimeseriesData(getTimeseriesDataRequest)
	if err != nil {
		fmt.Println("[Error]: Get timeseries data Failed with error: ", err)
		return
	}
	fmt.Println("[Info]: Get timeseries data succeed ! TimeseriesRows: ")
	for i := 0; i < len(getTimeseriesResp.GetRows()); i++ {
		tagsJson, _ := json.Marshal(getTimeseriesResp.GetRows()[i].GetTimeseriesKey().GetTags())
		fieldsJson, _ := json.Marshal(getTimeseriesResp.GetRows()[i].GetFieldsMap())
		fmt.Println("	[Info]: Row", i, ": [", getTimeseriesResp.GetRows()[i].GetTimeseriesKey().GetMeasurementName(),
			getTimeseriesResp.GetRows()[i].GetTimeseriesKey().GetDataSource(),
			tagsJson, "]",
			fieldsJson,
			getTimeseriesResp.GetRows()[i].GetTimeInus())
	}
	fmt.Println("[Info]: GetTimeseriesDataSample finished! RequestId: ", getTimeseriesResp.RequestId)
}

/**
 * QueryTimeseriesMetaSample queries the measurement, source, and tag information of specific timelines in a data table based on specified conditions, where the query conditions can be combined.
 */
func QueryTimeseriesMetaSample(client *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to query timeseries table meta !")

	// Construct multiple single query conditions
	measurementMetaQueryCondition := tablestore.NewMeasurementQueryCondition(tablestore.OP_GREATER_EQUAL, "")
	datasourceMetaQueryCondition := tablestore.NewDataSourceMetaQueryCondition(tablestore.OP_GREATER_EQUAL, "")
	tagMetaQueryCondition := tablestore.NewTagMetaQueryCondition(tablestore.OP_GREATER_THAN, "City", "")

	// Construct compound conditions
	compsiteMetaQueryCondition := tablestore.NewCompositeMetaQueryCondition(tablestore.OP_AND)
	compsiteMetaQueryCondition.AddSubConditions(measurementMetaQueryCondition)
	compsiteMetaQueryCondition.AddSubConditions(datasourceMetaQueryCondition)
	compsiteMetaQueryCondition.AddSubConditions(tagMetaQueryCondition)

	// Construct the query request
	queryTimeseriesMetaRequest := tablestore.NewQueryTimeseriesMetaRequest(timeseriesTableName)
	queryTimeseriesMetaRequest.SetCondition(compsiteMetaQueryCondition)
	queryTimeseriesMetaRequest.SetLimit(-1)

	// Call the client to execute the query request
	queryTimeseriesTableResponse, err := client.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	if err != nil {
		fmt.Println("[Error]: Query timeseries table meta failed with error: ", err)
		return
	}
	fmt.Println("	[Info]: Query timeseries table meta succeed: ")
	for i := 0; i < len(queryTimeseriesTableResponse.GetTimeseriesMetas()); i++ {
		curTimeseriesMeta := queryTimeseriesTableResponse.GetTimeseriesMetas()[i]
		fmt.Println("	[Info]: Meta_", i, ": ", "Measurement: ", curTimeseriesMeta.GetTimeseriesKey().GetMeasurementName(),
			"Source: ", curTimeseriesMeta.GetTimeseriesKey().GetDataSource(),
			"Tags: ", curTimeseriesMeta.GetTimeseriesKey().GetTags(),
			"Attrs: ", curTimeseriesMeta.GetAttributeSlice())
	}
	fmt.Println("[Info]: QueryTimeseriesMetaSample finished !")
}

/**
* UpdateTimeseriesMetaSample updates the Attributes information in the timeline.
 */
func UpdateTimeseriesMetaSample(tsClient *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to update timeseries meta !")

	PutTimeseriesDataSample(tsClient, timeseriesTableName)

	updateTimeseriesMetaRequest := tablestore.NewUpdateTimeseriesMetaRequest(timeseriesTableName)

	timeseriesKey := tablestore.NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("NETWORK")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City", "Hangzhou")
	timeseriesKey.AddTag("Region", "Xihu")

	timeseriesMeta := tablestore.NewTimeseriesMeta(timeseriesKey)
	//timeseriesMeta.SetUpdateTimeInUs(96400)
	timeseriesMeta.AddAttribute("NewRegion", "Yuhang")
	timeseriesMeta.AddAttribute("NewCity", "Shanghai")

	updateTimeseriesMetaRequest.AddTimeseriesMetas(timeseriesMeta)

	updateTimeseriesMetaResponse, err := tsClient.UpdateTimeseriesMeta(updateTimeseriesMetaRequest)
	if err != nil {
		fmt.Println("[Error]: Update timeseries meta failed with error: ", err)
		return
	}

	if len(updateTimeseriesMetaResponse.GetFailedRowResults()) > 0 {
		fmt.Println("	[Error]: Update timeseries meta failed row: ")
		for i := 0; i < len(updateTimeseriesMetaResponse.GetFailedRowResults()); i++ {
			fmt.Println("	[Error]: ", updateTimeseriesMetaResponse.GetFailedRowResults()[i].Index, updateTimeseriesMetaResponse.GetFailedRowResults()[i].Error)
		}
	}

	QueryTimeseriesMetaSample(tsClient, timeseriesTableName)

	fmt.Println("[Info]: UpdateTimeseriesMetaSample finished !")
}

// CreateTimeseriesTableWithAnalyticalStoreSample creates a timeseries table and also creates an analytical store.
func CreateTimeseriesTableWithAnalyticalStoreSample(tsClient *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to create timeseries table with analytical store !")

	// Create a time-series table
	meta := tablestore.NewTimeseriesTableMeta(timeseriesTableName)
	meta.SetTimeseriesTableOptions(tablestore.NewTimeseriesTableOptions(-1))
	createTimeseriesTableRequest := tablestore.NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	createTimeseriesTableRequest.SetAnalyticalStores([]*tablestore.TimeseriesAnalyticalStore{{
		StoreName:  "custom_analytical_store", // Analyze the storage name
		TimeToLive: proto.Int32(-1),           // Analyze the expiration time of stored data, in seconds, -1 means never expires
	}})
	_, err := tsClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Create timeseries table failed with error: ", err)
		return
	}

	fmt.Println("[Info]: Create timeseries table with analytical store succeed !")
}

// DescribeTimeseriesAnalyticalStoresSample lists all analytical stores under the timeseries table and prints the synchronization status and storage size of the analytical stores.
func DescribeTimeseriesAnalyticalStoresSample(tsClient *tablestore.TimeseriesClient, timeseriesTableName string) {
	fmt.Println("[Info]: Begin to describe timeseries analytical stores !")

	describeTimeseriesTableRequest := tablestore.NewDescribeTimeseriesTableRequset(timeseriesTableName)
	describeTimeseriesTableResponse, err := tsClient.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	if err != nil {
		fmt.Println("[Error]: Describe timeseries table failed with error: ", err)
		return
	}

	analyticalStores := describeTimeseriesTableResponse.GetAnalyticalStores()
	for _, analyticalStore := range analyticalStores {
		describeAnalyticalStoreRequest := tablestore.NewDescribeTimeseriesAnalyticalStoreRequest(timeseriesTableName, analyticalStore.StoreName)
		describeAnalyticalStoreResponse, err := tsClient.DescribeTimeseriesAnalyticalStore(describeAnalyticalStoreRequest)
		if err != nil {
			fmt.Println("[Error]: Describe analytical store failed with error: ", err)
			return
		}
		fmt.Println("	[Info]: StoreName: ", describeAnalyticalStoreResponse.AnalyticalStore.StoreName)
		fmt.Println("	[Info]: TimeToLive: ", describeAnalyticalStoreResponse.AnalyticalStore.TimeToLive)
		fmt.Println("	[Info]: SyncOption: ", describeAnalyticalStoreResponse.AnalyticalStore.SyncOption)
		syncStat := describeAnalyticalStoreResponse.SyncStat
		if syncStat != nil {
			fmt.Println("	[Info]: CurrentSyncTimestamp: ", syncStat.CurrentSyncTimestamp)
			fmt.Println("	[Info]: SyncPhase: ", syncStat.SyncPhase)
		}
		storageSize := describeAnalyticalStoreResponse.StorageSize
		if storageSize != nil {
			fmt.Println("	[Info]: Size: ", storageSize.Size)
			fmt.Println("	[Info]: Timestamp: ", storageSize.Timestamp)
		}
	}

	fmt.Println("[Info]: DescribeTimeseriesAnalyticalStoresSample finished !")
}
