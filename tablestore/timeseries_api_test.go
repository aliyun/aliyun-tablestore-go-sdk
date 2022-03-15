package tablestore

import (
	"fmt"
	. "gopkg.in/check.v1"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type TimeseriesSuite struct{}

var _ = Suite(&TimeseriesSuite{})

var defaultTimeseriesTableName = "defaulttimeseriestable"
var timeseriesTableNamePrefix string
var queryTimeseriesMetaTableName  = "querytimeseriestable"
var timeNow  = int64(1)
var timeseriesTableName  = "timeseriestable"

var timeseriesClient *TimeseriesClient
var invalidTimeseriesClient *TimeseriesClient

func (s *TimeseriesSuite) SetUpSuite(c *C) {
	endPoint := os.Getenv("OTS_TEST_ENDPOINT")
	instanceName := os.Getenv("OTS_TEST_INSTANCENAME")
	accessKeyId := os.Getenv("OTS_TEST_KEYID")
	accessKeySecret := os.Getenv("OTS_TEST_SECRET")

	timeseriesTableNamePrefix = strings.Replace(runtime.Version(), ".", "", -1)
	defaultTimeseriesTableName = timeseriesTableNamePrefix + defaultTimeseriesTableName

	timeseriesClient = NewTimeseriesClient(endPoint , instanceName , accessKeyId , accessKeySecret)
	invalidTimeseriesClient = NewTimeseriesClient(endPoint , instanceName , accessKeyId , "invalidsecret")
}

func PrepareTimeseriesTable(timeseriesTableName string) error {
	timeseriesTableMeta := NewTimeseriesTableMeta(timeseriesTableName)
	timeseriesTableOptions := NewTimeseriesTableOptions(864000)
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)

	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(timeseriesTableMeta)

	_ , err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	return err
}


func (s *TimeseriesSuite) TestDeleteAndCreateTimeseriesTable(c *C) {
	fmt.Println("[Info]: TestDeleteAndCreateTimeseriesTable start !")

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(timeNow))

	// 列出并删除所有时序表(注意：会删除所有已建立的时序表)
	listTimeseriesTables , err := timeseriesClient.ListTimeseriesTable()
	c.Check(err , Equals , nil)
	for _ , timeseriesTable := range listTimeseriesTables.GetTimeseriesTableNames() {
		// 删除表格
		deleteTimeseriesTableReq := NewDeleteTimeseriesTableRequest(timeseriesTable)
		_ , err = timeseriesClient.DeleteTimeseriesTable(deleteTimeseriesTableReq)
		c.Assert(err , Equals , nil)
		fmt.Println("	[Info]: Delete timeseries " , timeseriesTable , " succeed !")
	}

	// 删除不存在表格：返回table not exist错误。
	deleteTimeseriesReq := NewDeleteTimeseriesTableRequest(curTimeseriesTableName)
	_ , err = timeseriesClient.DeleteTimeseriesTable(deleteTimeseriesReq)
	c.Assert(err , NotNil)

	// 创建表格
	timeseriesTableMeta := NewTimeseriesTableMeta(curTimeseriesTableName)
	timeseriesTableOptions := NewTimeseriesTableOptions(86400)
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)

	createTimeseriesTableReq := NewCreateTimeseriesTableRequest()
	createTimeseriesTableReq.SetTimeseriesTableMeta(timeseriesTableMeta)

	_ , err = timeseriesClient.CreateTimeseriesTable(createTimeseriesTableReq)
	c.Check(err , Equals , nil)

	// 重复创建同一表格返回错误信息：服务端存在此表格
	timeseriesTableMeta = NewTimeseriesTableMeta(curTimeseriesTableName)
	timeseriesTableOptions = NewTimeseriesTableOptions(86400)
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)

	createTimeseriesTableReq = NewCreateTimeseriesTableRequest()
	createTimeseriesTableReq.SetTimeseriesTableMeta(timeseriesTableMeta)

	_ , err = timeseriesClient.CreateTimeseriesTable(createTimeseriesTableReq)
	c.Check(err , NotNil)

	fmt.Println("	[Info]: Create timeseries " , curTimeseriesTableName , " succeed !")
	fmt.Println("[Info]: TestDeleteAndCreateTimeseriesTable finished !")

	time.Sleep(time.Second * 30)		// 建立meta表
}

func (s *TimeseriesSuite) TestListTimeseriesTable(c *C) {
	fmt.Println("[Info]: TestListTimeseriesTable start !")

	listTimeseriesTables , err := timeseriesClient.ListTimeseriesTable()
	c.Check(err , Equals , nil)

	defaultTimeseriesTableExist := false
	for _ , timeseriesTable := range listTimeseriesTables.GetTimeseriesTableNames() {
		fmt.Println("	[Info]: Timeseries table Name: " , timeseriesTable)
		if timeseriesTable == defaultTimeseriesTableName {
			defaultTimeseriesTableExist = true
			break
		}
	}
	c.Check(defaultTimeseriesTableExist , Equals , false)

	fmt.Println("[Info]: TestListTimeseriesTable finished !")
}


func (s *TimeseriesSuite)TestUpdateAndDescribeTimeseriesTable(c *C) {
	fmt.Println("[Info]: TestUpdateAndDescribeTimeseriesTable start !")

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(timeNow))

	// 描述表信息
	describeTimeseriesTableReq := NewDescribeTimeseriesTableRequset(curTimeseriesTableName)
	describeResp , err := timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableReq)
	c.Assert(err , Equals , nil)
	c.Assert(describeResp , NotNil)

	// 更新表选项
	updateTimeseriesTableReq := NewUpdateTimeseriesTableRequest(curTimeseriesTableName)
	timeseriesTableOptions := NewTimeseriesTableOptions(965000)
	updateTimeseriesTableReq.SetTimeseriesTableOptions(timeseriesTableOptions)
	_ , err = timeseriesClient.UpdateTimeseriesTable(updateTimeseriesTableReq)
	c.Assert(err , Equals , nil)

	// 描述表信息
	describeTimeseriesTableReq = NewDescribeTimeseriesTableRequset(curTimeseriesTableName)
	describeResp , err = timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableReq)
	c.Assert(err , Equals , nil)
	c.Assert(describeResp , NotNil)
	c.Assert(describeResp.GetTimeseriesTableMeta().GetTimeseriesTableOPtions().GetTimeToLive() , Equals , updateTimeseriesTableReq.GetTimeseriesTableOptions().GetTimeToLive())

	fmt.Println("[Info]: TestUpdateAndDescribeTimeseriesTable finished !")
}

func (s *TimeseriesSuite) TestPutAndGetTimeseriesData(c *C) {
	fmt.Println("[Info]: TestPutAndGetTimeseriesData start !")

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(timeNow))
	PrepareTimeseriesTable(curTimeseriesTableName)

	time.Sleep(30 * time.Second)

	// 写入数据
	putTimeseriesDataRep := NewPutTimeseriesDataRequest(curTimeseriesTableName)

	var timeseriesKey *TimeseriesKey
	var timeseriesRow *TimeseriesRow
	for i := 0;  i < 10; i++ {
		timeseriesKey = NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("NETWORK")
		timeseriesKey.SetDataSource("127.0.0.1")
		timeseriesKey.AddTag("City" , "Hangzhou")
		timeseriesKey.AddTag("Region" , "Xihu")

		timeseriesRow = NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000 + int64(i * 10))
		timeseriesRow.AddField("lossRate" , NewColumnValue(ColumnType_DOUBLE , 0.68))
		timeseriesRow.AddField("netStatus" , NewColumnValue(ColumnType_BOOLEAN , true))
		timeseriesRow.AddField("dataSize" , NewColumnValue(ColumnType_INTEGER , 512))
		timeseriesRow.AddField("data" , NewColumnValue(ColumnType_BINARY , []byte("select * from NET")))
		timeseriesRow.AddField("program" , NewColumnValue(ColumnType_STRING , "tablestore.d"))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow)
	}

	for i := 0; i < 10; i++ {
		timeseriesKey1 := NewTimeseriesKey()
		timeseriesKey1.SetMeasurementName("CPU")
		timeseriesKey1.SetDataSource("127.0.0.1")
		timeseriesKey1.AddTag("City" , "Hangzhou")
		timeseriesKey1.AddTag("Region" , "Xihu")

		timeseriesRow1 := NewTimeseriesRow(timeseriesKey1)
		timeseriesRow1.SetTimeInus(time.Now().UnixNano() / 1000 + int64(i * 10 + 10))
		timeseriesRow1.AddField("temperature" , NewColumnValue(ColumnType_DOUBLE , 0.698))
		timeseriesRow1.AddField("runstatus" , NewColumnValue(ColumnType_BOOLEAN , true))
		timeseriesRow1.AddField("runminute" , NewColumnValue(ColumnType_INTEGER , 512))
		timeseriesRow1.AddField("program" , NewColumnValue(ColumnType_STRING , "tablestore.d"))
		timeseriesRow1.AddField("memdata", NewColumnValue(ColumnType_BINARY , []byte("a=123")))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow1)
	}

	putTimeseriesDataResp , err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRep)
	c.Assert(err , Equals , nil)
	c.Assert(len(putTimeseriesDataResp.GetFailedRowResults()) , Equals , 0)

	// 查询数据
	timeseriesKey = NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("NETWORK")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City" , "Hangzhou")
	timeseriesKey.AddTag("Region" , "Xihu")

	getTimeseriesDataReq := NewGetTimeseriesDataRequest(curTimeseriesTableName)
	getTimeseriesDataReq.SetTimeRange(0 , time.Now().UnixNano())
	getTimeseriesDataReq.SetTimeseriesKey(timeseriesKey)

	getTimeseriesDataResp , err := timeseriesClient.GetTimeseriesData(getTimeseriesDataReq)
	c.Assert(err , Equals , nil)
	c.Assert(len(getTimeseriesDataResp.GetRows()) , Equals , 10)
	for i := 0; i < len(getTimeseriesDataResp.GetRows()); i++ {
		row := getTimeseriesDataResp.GetRows()[i]
		c.Assert(row.GetTimeseriesKey().GetMeasurementName() , Equals , "NETWORK")
		c.Assert(row.GetTimeseriesKey().GetDataSource() , Equals , "127.0.0.1")
		c.Assert(len(row.GetTimeseriesKey().GetTags()) , Equals , 2)
		c.Assert(row.GetTimeseriesKey().GetTags()["City"] , Equals , "Hangzhou")
		c.Assert(row.GetTimeseriesKey().GetTags()["Region"] , Equals , "Xihu")

		c.Assert(string(row.GetFieldsMap()["data"].Value.([]byte)) , Equals , "select * from NET")
		c.Assert(row.GetFieldsMap()["netstatus"].Value.(bool) , Equals , true)
		c.Assert(row.GetFieldsMap()["program"].Value.(string) , Equals, "tablestore.d")
		c.Assert(row.GetFieldsMap()["lossrate"].Value.(float64) , Equals , 0.68)
		c.Assert(row.GetFieldsMap()["datasize"].Value.(int64) , Equals , int64(512))
	}

	// 查询数据
	timeseriesKey = NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("CPU")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City" , "Hangzhou")
	timeseriesKey.AddTag("Region" , "Xihu")

	getTimeseriesDataReq = NewGetTimeseriesDataRequest(curTimeseriesTableName)
	getTimeseriesDataReq.SetTimeRange(0 , time.Now().UnixNano())
	getTimeseriesDataReq.SetTimeseriesKey(timeseriesKey)

	getTimeseriesDataResp , err = timeseriesClient.GetTimeseriesData(getTimeseriesDataReq)
	c.Assert(err , Equals , nil)
	c.Assert(len(getTimeseriesDataResp.GetRows()) , Equals , 10)
	for i := 0; i < len(getTimeseriesDataResp.GetRows()); i++ {
		row := getTimeseriesDataResp.GetRows()[i]
		c.Assert(row.GetTimeseriesKey().GetMeasurementName() , Equals , "CPU")
		c.Assert(row.GetTimeseriesKey().GetDataSource() , Equals , "127.0.0.1")
		c.Assert(len(row.GetTimeseriesKey().GetTags()) , Equals , 2)
		c.Assert(row.GetTimeseriesKey().GetTags()["City"] , Equals , "Hangzhou")
		c.Assert(row.GetTimeseriesKey().GetTags()["Region"] , Equals , "Xihu")

		c.Assert(string(row.GetFieldsMap()["memdata"].Value.([]byte)) , Equals , "a=123")
		c.Assert(row.GetFieldsMap()["runstatus"].Value.(bool) , Equals , true)
		c.Assert(row.GetFieldsMap()["program"].Value.(string) , Equals , "tablestore.d")
		c.Assert(row.GetFieldsMap()["temperature"].Value.(float64) , NotNil)
		c.Assert(row.GetFieldsMap()["runminute"].Value.(int64) , NotNil)
	}

	fmt.Println("[Info]: TestPutAndGetTimeseriesData finished !")
}

func (s *TimeseriesSuite) TestQueryTimeseriesMeta(c *C) {
	fmt.Println("[Info]: TestQueryTimeseriesMeta start !")

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(timeNow))

	// 创建用于测试QueryTimeseriesMeta接口的时序表
	PrepareTimeseriesTable(curTimeseriesTableName)

	time.Sleep(30 * time.Second)

	putTimeseriesDataRep := NewPutTimeseriesDataRequest(curTimeseriesTableName)
	for i := 0;  i < 10; i++ {
		timeseriesKey := NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("NETWORK")
		timeseriesKey.SetDataSource("127.0.0.1")
		timeseriesKey.AddTag("City" , "Hangzhou")
		timeseriesKey.AddTag("Region" , "Xihu")

		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000 + int64(i * 10 + 10))
		timeseriesRow.AddField("LossRate" , NewColumnValue(ColumnType_DOUBLE , rand.Float64()))
		timeseriesRow.AddField("NetStatus" , NewColumnValue(ColumnType_BOOLEAN , true))
		timeseriesRow.AddField("DataSize" , NewColumnValue(ColumnType_INTEGER , rand.Int63()))
		timeseriesRow.AddField("Program" , NewColumnValue(ColumnType_STRING , "tablestore.d"))
		timeseriesRow.AddField("Data" , NewColumnValue(ColumnType_BINARY , []byte("0001000000100001000")))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow)
	}

	for i := 0; i < 10; i++ {
		timeseriesKey := NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("CPU")
		timeseriesKey.SetDataSource("127.0.0.1")
		timeseriesKey.AddTag("City" , "Hangzhou")
		timeseriesKey.AddTag("Region" , "Xihu")

		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000 + int64(i * 10 + 50))
		timeseriesRow.AddField("Temperature" , NewColumnValue(ColumnType_DOUBLE , rand.Float64()))
		timeseriesRow.AddField("RunStatus" , NewColumnValue(ColumnType_BOOLEAN , true))
		timeseriesRow.AddField("RunMinute" , NewColumnValue(ColumnType_INTEGER , rand.Int63()))
		timeseriesRow.AddField("Program" , NewColumnValue(ColumnType_STRING , "tablestore.d"))
		timeseriesRow.AddField("MemData", NewColumnValue(ColumnType_BINARY , []byte("select * from NET")))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow)
	}

	for i := 0; i < 10; i++ {
		timeseriesKey := NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("CPU")
		timeseriesKey.SetDataSource("127.0.0.1")
		timeseriesKey.AddTag("City" , "Hangzhou")
		timeseriesKey.AddTag("Region" , "YuHang")
		timeseriesKey.AddTag("Street" , "Zhuantang")

		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000 + int64(i * 10 + 100))
		timeseriesRow.AddField("Temperature" , NewColumnValue(ColumnType_DOUBLE , 0.68))
		timeseriesRow.AddField("RunStatus" , NewColumnValue(ColumnType_BOOLEAN , true))
		timeseriesRow.AddField("RunMinute" , NewColumnValue(ColumnType_INTEGER , 512))
		timeseriesRow.AddField("Program" , NewColumnValue(ColumnType_STRING , "tablestore.d"))
		timeseriesRow.AddField("MemData", NewColumnValue(ColumnType_BINARY , []byte("select * from NET")))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow)
	}
	putTimeseriesDataResp , err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRep)
	c.Assert(err , Equals , nil)
	c.Assert(len(putTimeseriesDataResp.GetFailedRowResults()) , Equals , 0)

	time.Sleep(time.Second * 30)		// 数据同步到meta表

	// 查询timeseriesMeta信息,单条件：measurementQueryMetaCondition
	measurementMetaQueryCondition := NewMeasurementQueryCondition(OP_GREATER_EQUAL , "")
	queryTimeseriesMetaReq := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq.SetCondition(measurementMetaQueryCondition)
	QueryTimeseriesMetaResp , err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq)
	c.Assert(err , Equals , nil)
	c.Assert(len(QueryTimeseriesMetaResp.GetTimeseriesMetas()) , Equals , 3)

	// 查询timeseriesMeta信息,单条件：sourceQueryMetaCondition
	sourceMetaQueryCondition := NewDataSourceMetaQueryCondition(OP_EQUAL , "127.0.0.1")
	queryTimeseriesMetaReq = NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq.SetCondition(sourceMetaQueryCondition)
	QueryTimeseriesMetaResp , err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq)
	c.Assert(err , Equals , nil)
	c.Assert(len(QueryTimeseriesMetaResp.GetTimeseriesMetas()) , Equals , 3)

	// 查询timeseriesMeta信息,单条件：tagQueryMetaCondition
	tagMetaQueryCondition := NewTagMetaQueryCondition(OP_EQUAL , "Street" , "Zhuantang")
	queryTimeseriesMetaReq = NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq.SetCondition(tagMetaQueryCondition)
	QueryTimeseriesMetaResp , err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq)
	c.Assert(err , Equals , nil)
	c.Assert(len(QueryTimeseriesMetaResp.GetTimeseriesMetas()) , Equals , 1)

	// 组合条件查询
	measurementMetaQueryCondition = NewMeasurementQueryCondition(OP_EQUAL , "CPU")
	sourceMetaQueryCondition = NewDataSourceMetaQueryCondition(OP_EQUAL , "127.0.0.1")
	tagMetaQueryCondition = NewTagMetaQueryCondition(OP_GREATER_EQUAL , "Region" , "Jiangning")

	// 设置measurement,source,tag条件
	compsiteMetaQueryCondition0 := NewCompositeMetaQueryCondition(OP_AND , measurementMetaQueryCondition , sourceMetaQueryCondition , tagMetaQueryCondition)
	queryTimeseriesMetaReq0 := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq0.SetCondition(compsiteMetaQueryCondition0)
	QueryTimeseriesMetaResp0 , err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq0)
	c.Assert(err , Equals , nil)
	c.Assert(len(QueryTimeseriesMetaResp0.GetTimeseriesMetas()) , Equals , 2)

	// 设置measurement，source条件
	compsiteMetaQueryCondition1 := NewCompositeMetaQueryCondition(OP_AND , measurementMetaQueryCondition , sourceMetaQueryCondition)
	queryTimeseriesMetaReq1 := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq1.SetCondition(compsiteMetaQueryCondition1)
	QueryTimeseriesMetaResp1 , err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq1)
	c.Assert(err , Equals , nil)
	c.Assert(len(QueryTimeseriesMetaResp1.GetTimeseriesMetas()) , Equals , 2)

	// 设置measurement，tag条件
	compsiteMetaQueryCondition2 := NewCompositeMetaQueryCondition(OP_AND , measurementMetaQueryCondition , tagMetaQueryCondition)
	queryTimeseriesMetaReq2 := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq2.SetCondition(compsiteMetaQueryCondition2)
	QueryTimeseriesMetaResp2 , err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq2)
	c.Assert(err , Equals , nil)
	c.Assert(len(QueryTimeseriesMetaResp2.GetTimeseriesMetas()) , Equals , 2)

	// 设置source，tag条件
	compsiteMetaQueryCondition3 := NewCompositeMetaQueryCondition(OP_OR , sourceMetaQueryCondition , tagMetaQueryCondition)
	queryTimeseriesMetaReq3 := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq3.SetCondition(compsiteMetaQueryCondition3)
	QueryTimeseriesMetaResp3 , err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq3)
	c.Assert(err , Equals , nil)
	c.Assert(len(QueryTimeseriesMetaResp3.GetTimeseriesMetas()) , Equals , 3)

	fmt.Println("[Info]: TestQueryTimeseriesMeta finished !")
}


func (s *TimeseriesSuite) TestUpdateTimeseriesMeta(c *C) {
	fmt.Println("[Info]: TestUpdateTimeseriesMeta start !")

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(time.Now().UnixNano()))

	// 创建用于测试UpdateTimeseriesMeta接口的时序表
	err := PrepareTimeseriesTable(curTimeseriesTableName)
	if err != nil {
		c.Fatal(err)
	}

	time.Sleep(60 * time.Second)

	timeseriesKey := NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("CPU")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("Province" , "Zhejiang")
	timeseriesKey.AddTag("City" , "Hangzhou")
	timeseriesKey.AddTag("Region" , "Xihu")

	timeseriesRow := NewTimeseriesRow(timeseriesKey)
	timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000)
	timeseriesRow.AddField("temperature" , NewColumnValue(ColumnType_DOUBLE , 98.5))
	timeseriesRow.AddField("status" , NewColumnValue(ColumnType_BOOLEAN , true))

	putTimeseriesDataRequest := NewPutTimeseriesDataRequest(curTimeseriesTableName)
	putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow)

	putTimeseriesDataResponse , err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRequest)
	c.Assert(err , Equals , nil)
	c.Assert(len(putTimeseriesDataResponse.GetFailedRowResults()) , Equals , 0)

	time.Sleep(10 * time.Second)

	// 查询meta
	measurementQueryCondition := NewMeasurementQueryCondition(OP_EQUAL , "CPU")
	queryTimeseriesMetaRequest := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaRequest.SetCondition(measurementQueryCondition)

	queryTimeseriesMetaResponse , err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err , Equals , nil)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()) , Equals , 1)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()[0].GetAttributes()) , Equals , 0)

	// 更新meta
	timeseriesMeta := NewTimeseriesMeta(timeseriesKey)
	timeseriesMeta.AddAttribute("NewRegion" , "Yuhang")

	updateTimeseriesMetaRequest := NewUpdateTimeseriesMetaRequest(curTimeseriesTableName)
	updateTimeseriesMetaRequest.AddTimeseriesMetas(timeseriesMeta)

	updateTimeseriesMetaResponse , err := timeseriesClient.UpdateTimeseriesMeta(updateTimeseriesMetaRequest)
	c.Assert(err , Equals , nil)
	c.Assert(len(updateTimeseriesMetaResponse.GetFailedRowResults()) , Equals , 0)

	time.Sleep(10 * time.Second)

	// 再次查询meta
	measurementQueryCondition = NewMeasurementQueryCondition(OP_EQUAL , "CPU")
	queryTimeseriesMetaRequest = NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaRequest.SetCondition(measurementQueryCondition)

	queryTimeseriesMetaResponse , err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err , Equals , nil)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()) , Equals , 1)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()[0].GetAttributes()) , Equals , 1)
}


func (s *TimeseriesSuite) TestDeleteTimeseriesMeta(c *C) {
	fmt.Println("[Info]: TestDeleteTimeseriesMeta start !")

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(time.Now().UnixNano()))

	// 创建用于测试DeleteTimeseriesMeta接口的时序表
	err := PrepareTimeseriesTable(curTimeseriesTableName)
	if err != nil {
		c.Fatal(err)
	}
	time.Sleep(60 * time.Second)

	putTimeseriesDataRequest := NewPutTimeseriesDataRequest(curTimeseriesTableName)
	for i := 0; i < 100; i++ {
		timeseriesKey := NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("CPU")
		timeseriesKey.SetDataSource("source_" + strconv.Itoa(i))
		timeseriesKey.AddTag("Province" , "浙江")
		timeseriesKey.AddTag("City" , "杭州")

		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000)
		timeseriesRow.AddField("temperature" , NewColumnValue(ColumnType_DOUBLE , 98.5))
		timeseriesRow.AddField("status" , NewColumnValue(ColumnType_BOOLEAN , true))
		putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow)
	}
	putTimeseriesDataResponse , err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRequest)
	c.Assert(err , Equals , nil)
	c.Assert(len(putTimeseriesDataResponse.GetFailedRowResults()) , Equals , 0)

	time.Sleep(20 * time.Second)

	// 查询meta
	measurementQueryCondition := NewMeasurementQueryCondition(OP_EQUAL , "CPU")
	queryTimeseriesMetaRequest := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaRequest.SetCondition(measurementQueryCondition)

	queryTimeseriesMetaResponse , err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err , Equals , nil)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()) , Equals , 100)

	// 删除meta
	deleteTimeseriesMetaRequest := NewDeleteTimeseriesMetaRequest(curTimeseriesTableName)
	for i := 0; i < len(queryTimeseriesMetaResponse.GetTimeseriesMetas()); i++ {
		deleteTimeseriesMetaRequest.AddTimeseriesKeys(queryTimeseriesMetaResponse.GetTimeseriesMetas()[i].GetTimeseriesKey())
	}
	deleteTimeseriesMetaResponse , err := timeseriesClient.DeleteTimeseriesMeta(deleteTimeseriesMetaRequest)
	c.Assert(err , Equals , nil)
	c.Assert(len(deleteTimeseriesMetaResponse.GetFailedRowResults()) , Equals , 0)

	time.Sleep(20 * time.Second)

	// 再次查询meta
	measurementQueryCondition = NewMeasurementQueryCondition(OP_EQUAL , "CPU")
	queryTimeseriesMetaRequest = NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaRequest.SetCondition(measurementQueryCondition)

	queryTimeseriesMetaResponse , err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err , Equals , nil)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()) , Equals , 0)
}