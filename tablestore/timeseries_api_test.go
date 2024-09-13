package tablestore

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/aliyun/aliyun-tablestore-go-sdk/testConfig"
	"github.com/golang/protobuf/proto"
	. "gopkg.in/check.v1"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type TimeseriesSuite struct{}

var _ = Suite(&TimeseriesSuite{})

var defaultTimeseriesTableName = "defaulttimeseriestable"
var timeseriesTableNamePrefix string
var queryTimeseriesMetaTableName = "querytimeseriestable"
var timeNow = int64(1)
var timeseriesTableName = "timeseriestable"

var tableStoreClient *TableStoreClient
var timeseriesClient *TimeseriesClient
var invalidTimeseriesClient *TimeseriesClient

func (s *TimeseriesSuite) SetUpSuite(c *C) {
	endpoint := testConfig.OtsEndpoint
	instanceName := testConfig.InstanceName
	accessKeyId := testConfig.OtsAccessId
	accessKeySecret := testConfig.OtsAccessKey

	timeseriesTableNamePrefix = strings.Replace(runtime.Version(), ".", "", -1)
	defaultTimeseriesTableName = timeseriesTableNamePrefix + defaultTimeseriesTableName

	tableStoreClient = NewClient(endpoint, instanceName, accessKeyId, accessKeySecret)
	timeseriesClient = NewTimeseriesClient(endpoint, instanceName, accessKeyId, accessKeySecret)
	invalidTimeseriesClient = NewTimeseriesClient(endpoint, instanceName, accessKeyId, "invalidsecret")
}

func (s *TimeseriesSuite) TearDownSuite(c *C) {
	s.deleteTimeseriesTable("test_analytical_store1")
	s.deleteTimeseriesTable("test_analytical_store2")
	s.deleteTimeseriesTable("test_analytical_store3")
	s.deleteTimeseriesTable("test_create_and_delete_analytical_store1")
	s.deleteTimeseriesTable("test_create_and_delete_analytical_store2")
	s.deleteTimeseriesTable("test_update_analytical_store")
	s.deleteTimeseriesTable("test_describe_index_sync_phase")
	s.deleteTimeseriesTable("test_custom_primary_keys")
	s.deleteTimeseriesTable("test_custom_primary_keys_meta")
	s.deleteTimeseriesTable("test_custom_primary_keys_tags")
	s.deleteTimeseriesTable("test_lastpoint_index_create_delete")
}

func (s *TimeseriesSuite) deleteTimeseriesTable(tableName string) {
	_, _ = timeseriesClient.DeleteTimeseriesTable(NewDeleteTimeseriesTableRequest(tableName))
}

func PrepareTimeseriesTable(timeseriesTableName string) error {
	timeseriesTableMeta := NewTimeseriesTableMeta(timeseriesTableName)
	timeseriesTableOptions := NewTimeseriesTableOptions(864000)
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)

	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(timeseriesTableMeta)

	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	return err
}

func (s *TimeseriesSuite) TestDeleteAndCreateTimeseriesTable(c *C) {
	fmt.Println("[Info]: TestDeleteAndCreateTimeseriesTable start !")

	// 列出并删除所有时序表(注意：会删除所有已建立的时序表)
	listTimeseriesTables, err := timeseriesClient.ListTimeseriesTable()
	c.Check(err, Equals, nil)
	for _, timeseriesTable := range listTimeseriesTables.GetTimeseriesTableNames() {
		// 删除表格
		deleteTimeseriesTableReq := NewDeleteTimeseriesTableRequest(timeseriesTable)
		_, err = timeseriesClient.DeleteTimeseriesTable(deleteTimeseriesTableReq)
		c.Assert(err, Equals, nil)
		fmt.Println("	[Info]: Delete timeseries ", timeseriesTable, " succeed !")
	}

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(timeNow))
	// 删除不存在表格：返回table not exist错误。
	deleteTimeseriesReq := NewDeleteTimeseriesTableRequest(curTimeseriesTableName)
	_, err = timeseriesClient.DeleteTimeseriesTable(deleteTimeseriesReq)
	c.Assert(err, NotNil)

	// 创建表格
	timeseriesTableMeta := NewTimeseriesTableMeta(curTimeseriesTableName)
	timeseriesTableOptions := NewTimeseriesTableOptions(86400)
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)

	createTimeseriesTableReq := NewCreateTimeseriesTableRequest()
	createTimeseriesTableReq.SetTimeseriesTableMeta(timeseriesTableMeta)

	_, err = timeseriesClient.CreateTimeseriesTable(createTimeseriesTableReq)
	c.Check(err, Equals, nil)

	// 重复创建同一表格返回错误信息：服务端存在此表格
	timeseriesTableMeta = NewTimeseriesTableMeta(curTimeseriesTableName)
	timeseriesTableOptions = NewTimeseriesTableOptions(86400)
	timeseriesTableMeta.SetTimeseriesTableOptions(timeseriesTableOptions)

	createTimeseriesTableReq = NewCreateTimeseriesTableRequest()
	createTimeseriesTableReq.SetTimeseriesTableMeta(timeseriesTableMeta)

	_, err = timeseriesClient.CreateTimeseriesTable(createTimeseriesTableReq)
	c.Check(err, NotNil)

	fmt.Println("	[Info]: Create timeseries ", curTimeseriesTableName, " succeed !")
	fmt.Println("[Info]: TestDeleteAndCreateTimeseriesTable finished !")

	time.Sleep(time.Second * 30) // 建立meta表
}

func (s *TimeseriesSuite) TestListTimeseriesTable(c *C) {
	fmt.Println("[Info]: TestListTimeseriesTable start !")

	listTimeseriesTables, err := timeseriesClient.ListTimeseriesTable()
	c.Check(err, Equals, nil)

	defaultTimeseriesTableExist := false
	for _, timeseriesTable := range listTimeseriesTables.GetTimeseriesTableNames() {
		fmt.Println("	[Info]: Timeseries table Name: ", timeseriesTable)
		if timeseriesTable == defaultTimeseriesTableName {
			defaultTimeseriesTableExist = true
			break
		}
	}
	c.Check(defaultTimeseriesTableExist, Equals, false)

	fmt.Println("[Info]: TestListTimeseriesTable finished !")
}

func (s *TimeseriesSuite) TestUpdateAndDescribeTimeseriesTable(c *C) {
	fmt.Println("[Info]: TestUpdateAndDescribeTimeseriesTable start !")

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(timeNow))

	// 描述表信息
	describeTimeseriesTableReq := NewDescribeTimeseriesTableRequset(curTimeseriesTableName)
	describeResp, err := timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableReq)
	c.Assert(err, Equals, nil)
	c.Assert(describeResp, NotNil)

	// 更新表选项
	updateTimeseriesTableReq := NewUpdateTimeseriesTableRequest(curTimeseriesTableName)
	timeseriesTableOptions := NewTimeseriesTableOptions(965000)
	updateTimeseriesTableReq.SetTimeseriesTableOptions(timeseriesTableOptions)
	_, err = timeseriesClient.UpdateTimeseriesTable(updateTimeseriesTableReq)
	c.Assert(err, Equals, nil)

	// 描述表信息
	describeTimeseriesTableReq = NewDescribeTimeseriesTableRequset(curTimeseriesTableName)
	describeResp, err = timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableReq)
	c.Assert(err, Equals, nil)
	c.Assert(describeResp, NotNil)
	c.Assert(describeResp.GetTimeseriesTableMeta().GetTimeseriesTableOPtions().GetTimeToLive(), Equals, updateTimeseriesTableReq.GetTimeseriesTableOptions().GetTimeToLive())

	fmt.Println("[Info]: TestUpdateAndDescribeTimeseriesTable finished !")
}

func (s *TimeseriesSuite) TestPutAndGetTimeseriesData_FlatBuffer(c *C) {
	RowsSerializeType = otsprotocol.RowsSerializeType_RST_FLAT_BUFFER
	s.testPutAndGetTimeseriesData(c)
}

func (s *TimeseriesSuite) TestPutAndGetTimeseriesData_ProtoBuffer(c *C) {
	RowsSerializeType = otsprotocol.RowsSerializeType_RST_PROTO_BUFFER
	s.testPutAndGetTimeseriesData(c)
}

func (s *TimeseriesSuite) testPutAndGetTimeseriesData(c *C) {
	fmt.Println("[Info]: TestPutAndGetTimeseriesData start !")

	curTimeseriesTableName := timeseriesTableNamePrefix + timeseriesTableName + strconv.Itoa(int(timeNow))
	timeseriesClient.DeleteTimeseriesTable(NewDeleteTimeseriesTableRequest(curTimeseriesTableName))
	err := PrepareTimeseriesTable(curTimeseriesTableName)
	if err != nil {
		c.Fatal(err)
	}

	time.Sleep(30 * time.Second)

	// 写入数据
	putTimeseriesDataRep := NewPutTimeseriesDataRequest(curTimeseriesTableName)

	var timeseriesKey *TimeseriesKey
	var timeseriesRow *TimeseriesRow
	for i := 0; i < 10; i++ {
		timeseriesKey = NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("NETWORK")
		timeseriesKey.SetDataSource("127.0.0.1")
		timeseriesKey.AddTag("City", "Hangzhou")
		timeseriesKey.AddTag("Region", "Xihu")

		timeseriesRow = NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano()/1000 + int64(i*10))
		timeseriesRow.AddField("lossRate", NewColumnValue(ColumnType_DOUBLE, 0.68))
		timeseriesRow.AddField("netStatus", NewColumnValue(ColumnType_BOOLEAN, true))
		timeseriesRow.AddField("dataSize", NewColumnValue(ColumnType_INTEGER, 512))
		timeseriesRow.AddField("data", NewColumnValue(ColumnType_BINARY, []byte("select * from NET")))
		timeseriesRow.AddField("program", NewColumnValue(ColumnType_STRING, "tablestore.d"))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow)
	}

	for i := 0; i < 10; i++ {
		timeseriesKey1 := NewTimeseriesKey()
		timeseriesKey1.SetMeasurementName("CPU")
		timeseriesKey1.SetDataSource("127.0.0.1")
		timeseriesKey1.AddTag("City", "Hangzhou")
		timeseriesKey1.AddTag("Region", "Xihu")

		timeseriesRow1 := NewTimeseriesRow(timeseriesKey1)
		timeseriesRow1.SetTimeInus(time.Now().UnixNano()/1000 + int64(i*10+10))
		timeseriesRow1.AddField("temperature", NewColumnValue(ColumnType_DOUBLE, 0.698))
		timeseriesRow1.AddField("runstatus", NewColumnValue(ColumnType_BOOLEAN, true))
		timeseriesRow1.AddField("runminute", NewColumnValue(ColumnType_INTEGER, 512))
		timeseriesRow1.AddField("program", NewColumnValue(ColumnType_STRING, "tablestore.d"))
		timeseriesRow1.AddField("memdata", NewColumnValue(ColumnType_BINARY, []byte("a=123")))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow1)
	}

	putTimeseriesDataResp, err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRep)
	c.Assert(err, Equals, nil)
	c.Assert(len(putTimeseriesDataResp.GetFailedRowResults()), Equals, 0)

	// 查询数据
	timeseriesKey = NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("NETWORK")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City", "Hangzhou")
	timeseriesKey.AddTag("Region", "Xihu")

	getTimeseriesDataReq := NewGetTimeseriesDataRequest(curTimeseriesTableName)
	getTimeseriesDataReq.SetTimeRange(0, time.Now().UnixNano())
	getTimeseriesDataReq.SetTimeseriesKey(timeseriesKey)

	getTimeseriesDataResp, err := timeseriesClient.GetTimeseriesData(getTimeseriesDataReq)
	c.Assert(err, Equals, nil)
	c.Assert(len(getTimeseriesDataResp.GetRows()), Equals, 10)
	lastRowTime := int64(0)
	for i := 0; i < len(getTimeseriesDataResp.GetRows()); i++ {
		row := getTimeseriesDataResp.GetRows()[i]
		c.Assert(row.GetTimeseriesKey().GetMeasurementName(), Equals, "NETWORK")
		c.Assert(row.GetTimeseriesKey().GetDataSource(), Equals, "127.0.0.1")
		c.Assert(len(row.GetTimeseriesKey().GetTags()), Equals, 2)
		c.Assert(row.GetTimeseriesKey().GetTags()["City"], Equals, "Hangzhou")
		c.Assert(row.GetTimeseriesKey().GetTags()["Region"], Equals, "Xihu")
		c.Assert(row.GetTimeInus() > lastRowTime, Equals, true)
		lastRowTime = row.GetTimeInus()
		c.Assert(string(row.GetFieldsMap()["data"].Value.([]byte)), Equals, "select * from NET")
		c.Assert(row.GetFieldsMap()["netstatus"].Value.(bool), Equals, true)
		c.Assert(row.GetFieldsMap()["program"].Value.(string), Equals, "tablestore.d")
		c.Assert(row.GetFieldsMap()["lossrate"].Value.(float64), Equals, 0.68)
		c.Assert(row.GetFieldsMap()["datasize"].Value.(int64), Equals, int64(512))
	}

	// 逆序查询数据
	getTimeseriesDataReq = NewGetTimeseriesDataRequest(curTimeseriesTableName)
	getTimeseriesDataReq.SetTimeRange(0, time.Now().UnixNano())
	getTimeseriesDataReq.SetTimeseriesKey(timeseriesKey)
	getTimeseriesDataReq.SetBackward(true)

	getTimeseriesDataResp, err = timeseriesClient.GetTimeseriesData(getTimeseriesDataReq)
	c.Assert(err, Equals, nil)
	c.Assert(len(getTimeseriesDataResp.GetRows()), Equals, 10)
	lastRowTime = int64(math.MaxInt64)
	for i := 0; i < len(getTimeseriesDataResp.GetRows()); i++ {
		row := getTimeseriesDataResp.GetRows()[i]
		c.Assert(row.GetTimeseriesKey().GetMeasurementName(), Equals, "NETWORK")
		c.Assert(row.GetTimeseriesKey().GetDataSource(), Equals, "127.0.0.1")
		c.Assert(len(row.GetTimeseriesKey().GetTags()), Equals, 2)
		c.Assert(row.GetTimeseriesKey().GetTags()["City"], Equals, "Hangzhou")
		c.Assert(row.GetTimeseriesKey().GetTags()["Region"], Equals, "Xihu")
		c.Assert(row.GetTimeInus() < lastRowTime, Equals, true) // 逆序
		lastRowTime = row.GetTimeInus()
		c.Assert(string(row.GetFieldsMap()["data"].Value.([]byte)), Equals, "select * from NET")
		c.Assert(row.GetFieldsMap()["netstatus"].Value.(bool), Equals, true)
		c.Assert(row.GetFieldsMap()["program"].Value.(string), Equals, "tablestore.d")
		c.Assert(row.GetFieldsMap()["lossrate"].Value.(float64), Equals, 0.68)
		c.Assert(row.GetFieldsMap()["datasize"].Value.(int64), Equals, int64(512))
	}

	// 查询数据
	timeseriesKey = NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("CPU")
	timeseriesKey.SetDataSource("127.0.0.1")
	timeseriesKey.AddTag("City", "Hangzhou")
	timeseriesKey.AddTag("Region", "Xihu")

	getTimeseriesDataReq = NewGetTimeseriesDataRequest(curTimeseriesTableName)
	getTimeseriesDataReq.SetTimeRange(0, time.Now().UnixNano())
	getTimeseriesDataReq.SetTimeseriesKey(timeseriesKey)

	getTimeseriesDataResp, err = timeseriesClient.GetTimeseriesData(getTimeseriesDataReq)
	c.Assert(err, Equals, nil)
	c.Assert(len(getTimeseriesDataResp.GetRows()), Equals, 10)
	for i := 0; i < len(getTimeseriesDataResp.GetRows()); i++ {
		row := getTimeseriesDataResp.GetRows()[i]
		c.Assert(row.GetTimeseriesKey().GetMeasurementName(), Equals, "CPU")
		c.Assert(row.GetTimeseriesKey().GetDataSource(), Equals, "127.0.0.1")
		c.Assert(len(row.GetTimeseriesKey().GetTags()), Equals, 2)
		c.Assert(row.GetTimeseriesKey().GetTags()["City"], Equals, "Hangzhou")
		c.Assert(row.GetTimeseriesKey().GetTags()["Region"], Equals, "Xihu")

		c.Assert(string(row.GetFieldsMap()["memdata"].Value.([]byte)), Equals, "a=123")
		c.Assert(row.GetFieldsMap()["runstatus"].Value.(bool), Equals, true)
		c.Assert(row.GetFieldsMap()["program"].Value.(string), Equals, "tablestore.d")
		c.Assert(row.GetFieldsMap()["temperature"].Value.(float64), NotNil)
		c.Assert(row.GetFieldsMap()["runminute"].Value.(int64), NotNil)
	}

	// 指定列查询数据
	getTimeseriesDataReq = NewGetTimeseriesDataRequest(curTimeseriesTableName)
	getTimeseriesDataReq.SetTimeRange(0, time.Now().UnixNano())
	getTimeseriesDataReq.SetTimeseriesKey(timeseriesKey)
	getTimeseriesDataReq.AddFieldToGet(&FieldToGet{
		Name: "memdata",
		Type: ColumnType_BINARY,
	})
	getTimeseriesDataReq.AddFieldToGet(&FieldToGet{
		Name: "program",
		Type: ColumnType_STRING,
	})
	getTimeseriesDataReq.AddFieldToGet(&FieldToGet{
		Name: "runminute",
		Type: ColumnType_INTEGER,
	})
	getTimeseriesDataReq.AddFieldToGet(&FieldToGet{
		Name: "temperature",
		Type: ColumnType_DOUBLE,
	})

	getTimeseriesDataResp, err = timeseriesClient.GetTimeseriesData(getTimeseriesDataReq)
	c.Assert(err, Equals, nil)
	c.Assert(len(getTimeseriesDataResp.GetRows()), Equals, 10)
	for i := 0; i < len(getTimeseriesDataResp.GetRows()); i++ {
		row := getTimeseriesDataResp.GetRows()[i]
		c.Assert(row.GetTimeseriesKey().GetMeasurementName(), Equals, "CPU")
		c.Assert(row.GetTimeseriesKey().GetDataSource(), Equals, "127.0.0.1")
		c.Assert(len(row.GetTimeseriesKey().GetTags()), Equals, 2)
		c.Assert(row.GetTimeseriesKey().GetTags()["City"], Equals, "Hangzhou")
		c.Assert(row.GetTimeseriesKey().GetTags()["Region"], Equals, "Xihu")
		c.Assert(len(row.GetFieldsMap()), Equals, 4)
		c.Assert(string(row.GetFieldsMap()["memdata"].Value.([]byte)), Equals, "a=123")
		c.Assert(row.GetFieldsMap()["program"].Value.(string), Equals, "tablestore.d")
		c.Assert(row.GetFieldsMap()["temperature"].Value.(float64), NotNil)
		c.Assert(row.GetFieldsMap()["runminute"].Value.(int64), NotNil)
		_, ok := row.GetFieldsMap()["runstatus"]
		c.Assert(ok, Equals, false)
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
	for i := 0; i < 10; i++ {
		timeseriesKey := NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("NETWORK")
		timeseriesKey.SetDataSource("127.0.0.1")
		timeseriesKey.AddTag("City", "Hangzhou")
		timeseriesKey.AddTag("Region", "Xihu")

		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano()/1000 + int64(i*10+10))
		timeseriesRow.AddField("LossRate", NewColumnValue(ColumnType_DOUBLE, rand.Float64()))
		timeseriesRow.AddField("NetStatus", NewColumnValue(ColumnType_BOOLEAN, true))
		timeseriesRow.AddField("DataSize", NewColumnValue(ColumnType_INTEGER, rand.Int63()))
		timeseriesRow.AddField("Program", NewColumnValue(ColumnType_STRING, "tablestore.d"))
		timeseriesRow.AddField("Data", NewColumnValue(ColumnType_BINARY, []byte("0001000000100001000")))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow)
	}

	for i := 0; i < 10; i++ {
		timeseriesKey := NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("CPU")
		timeseriesKey.SetDataSource("127.0.0.1")
		timeseriesKey.AddTag("City", "Hangzhou")
		timeseriesKey.AddTag("Region", "Xihu")

		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano()/1000 + int64(i*10+50))
		timeseriesRow.AddField("Temperature", NewColumnValue(ColumnType_DOUBLE, rand.Float64()))
		timeseriesRow.AddField("RunStatus", NewColumnValue(ColumnType_BOOLEAN, true))
		timeseriesRow.AddField("RunMinute", NewColumnValue(ColumnType_INTEGER, rand.Int63()))
		timeseriesRow.AddField("Program", NewColumnValue(ColumnType_STRING, "tablestore.d"))
		timeseriesRow.AddField("MemData", NewColumnValue(ColumnType_BINARY, []byte("select * from NET")))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow)
	}

	for i := 0; i < 10; i++ {
		timeseriesKey := NewTimeseriesKey()
		timeseriesKey.SetMeasurementName("CPU")
		timeseriesKey.SetDataSource("127.0.0.1")
		timeseriesKey.AddTag("City", "Hangzhou")
		timeseriesKey.AddTag("Region", "YuHang")
		timeseriesKey.AddTag("Street", "Zhuantang")

		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano()/1000 + int64(i*10+100))
		timeseriesRow.AddField("Temperature", NewColumnValue(ColumnType_DOUBLE, 0.68))
		timeseriesRow.AddField("RunStatus", NewColumnValue(ColumnType_BOOLEAN, true))
		timeseriesRow.AddField("RunMinute", NewColumnValue(ColumnType_INTEGER, 512))
		timeseriesRow.AddField("Program", NewColumnValue(ColumnType_STRING, "tablestore.d"))
		timeseriesRow.AddField("MemData", NewColumnValue(ColumnType_BINARY, []byte("select * from NET")))

		putTimeseriesDataRep.AddTimeseriesRows(timeseriesRow)
	}
	putTimeseriesDataResp, err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRep)
	c.Assert(err, Equals, nil)
	c.Assert(len(putTimeseriesDataResp.GetFailedRowResults()), Equals, 0)

	time.Sleep(time.Second * 30) // 数据同步到meta表

	// 查询timeseriesMeta信息,单条件：measurementQueryMetaCondition
	measurementMetaQueryCondition := NewMeasurementQueryCondition(OP_GREATER_EQUAL, "")
	queryTimeseriesMetaReq := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq.SetCondition(measurementMetaQueryCondition)
	QueryTimeseriesMetaResp, err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq)
	c.Assert(err, Equals, nil)
	c.Assert(len(QueryTimeseriesMetaResp.GetTimeseriesMetas()), Equals, 3)

	// 查询timeseriesMeta信息,单条件：sourceQueryMetaCondition
	sourceMetaQueryCondition := NewDataSourceMetaQueryCondition(OP_EQUAL, "127.0.0.1")
	queryTimeseriesMetaReq = NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq.SetCondition(sourceMetaQueryCondition)
	QueryTimeseriesMetaResp, err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq)
	c.Assert(err, Equals, nil)
	c.Assert(len(QueryTimeseriesMetaResp.GetTimeseriesMetas()), Equals, 3)

	// 查询timeseriesMeta信息,单条件：tagQueryMetaCondition
	tagMetaQueryCondition := NewTagMetaQueryCondition(OP_EQUAL, "Street", "Zhuantang")
	queryTimeseriesMetaReq = NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq.SetCondition(tagMetaQueryCondition)
	QueryTimeseriesMetaResp, err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq)
	c.Assert(err, Equals, nil)
	metas := QueryTimeseriesMetaResp.GetTimeseriesMetas()
	c.Assert(len(metas), Equals, 1)
	key := metas[0].GetTimeseriesKey()
	c.Assert(key.GetMeasurementName(), Equals, "CPU")
	c.Assert(key.GetDataSource(), Equals, "127.0.0.1")
	c.Assert(len(key.GetTags()), Equals, 3)
	c.Assert(key.GetTags()["City"], Equals, "Hangzhou")
	c.Assert(key.GetTags()["Region"], Equals, "YuHang")
	c.Assert(key.GetTags()["Street"], Equals, "Zhuantang")

	// 组合条件查询
	measurementMetaQueryCondition = NewMeasurementQueryCondition(OP_EQUAL, "CPU")
	sourceMetaQueryCondition = NewDataSourceMetaQueryCondition(OP_EQUAL, "127.0.0.1")
	tagMetaQueryCondition = NewTagMetaQueryCondition(OP_GREATER_EQUAL, "Region", "Jiangning")

	// 设置measurement,source,tag条件
	compsiteMetaQueryCondition0 := NewCompositeMetaQueryCondition(OP_AND, measurementMetaQueryCondition, sourceMetaQueryCondition, tagMetaQueryCondition)
	queryTimeseriesMetaReq0 := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq0.SetCondition(compsiteMetaQueryCondition0)
	QueryTimeseriesMetaResp0, err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq0)
	c.Assert(err, Equals, nil)
	c.Assert(len(QueryTimeseriesMetaResp0.GetTimeseriesMetas()), Equals, 2)

	// 设置measurement，source条件
	compsiteMetaQueryCondition1 := NewCompositeMetaQueryCondition(OP_AND, measurementMetaQueryCondition, sourceMetaQueryCondition)
	queryTimeseriesMetaReq1 := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq1.SetCondition(compsiteMetaQueryCondition1)
	QueryTimeseriesMetaResp1, err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq1)
	c.Assert(err, Equals, nil)
	c.Assert(len(QueryTimeseriesMetaResp1.GetTimeseriesMetas()), Equals, 2)

	// 设置measurement，tag条件
	compsiteMetaQueryCondition2 := NewCompositeMetaQueryCondition(OP_AND, measurementMetaQueryCondition, tagMetaQueryCondition)
	queryTimeseriesMetaReq2 := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq2.SetCondition(compsiteMetaQueryCondition2)
	QueryTimeseriesMetaResp2, err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq2)
	c.Assert(err, Equals, nil)
	c.Assert(len(QueryTimeseriesMetaResp2.GetTimeseriesMetas()), Equals, 2)

	// 设置source，tag条件
	compsiteMetaQueryCondition3 := NewCompositeMetaQueryCondition(OP_OR, sourceMetaQueryCondition, tagMetaQueryCondition)
	queryTimeseriesMetaReq3 := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaReq3.SetCondition(compsiteMetaQueryCondition3)
	QueryTimeseriesMetaResp3, err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaReq3)
	c.Assert(err, Equals, nil)
	c.Assert(len(QueryTimeseriesMetaResp3.GetTimeseriesMetas()), Equals, 3)

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
	timeseriesKey.AddTag("Province", "Zhejiang")
	timeseriesKey.AddTag("City", "Hangzhou")
	timeseriesKey.AddTag("Region", "Xihu")

	timeseriesRow := NewTimeseriesRow(timeseriesKey)
	timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000)
	timeseriesRow.AddField("temperature", NewColumnValue(ColumnType_DOUBLE, 98.5))
	timeseriesRow.AddField("status", NewColumnValue(ColumnType_BOOLEAN, true))

	putTimeseriesDataRequest := NewPutTimeseriesDataRequest(curTimeseriesTableName)
	putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow)

	putTimeseriesDataResponse, err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(putTimeseriesDataResponse.GetFailedRowResults()), Equals, 0)

	time.Sleep(10 * time.Second)

	// 查询meta
	measurementQueryCondition := NewMeasurementQueryCondition(OP_EQUAL, "CPU")
	queryTimeseriesMetaRequest := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaRequest.SetCondition(measurementQueryCondition)

	queryTimeseriesMetaResponse, err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()), Equals, 1)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()[0].GetAttributes()), Equals, 0)

	// 更新meta
	timeseriesMeta := NewTimeseriesMeta(timeseriesKey)
	timeseriesMeta.AddAttribute("NewRegion", "Yuhang")

	updateTimeseriesMetaRequest := NewUpdateTimeseriesMetaRequest(curTimeseriesTableName)
	updateTimeseriesMetaRequest.AddTimeseriesMetas(timeseriesMeta)

	updateTimeseriesMetaResponse, err := timeseriesClient.UpdateTimeseriesMeta(updateTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(updateTimeseriesMetaResponse.GetFailedRowResults()), Equals, 0)

	time.Sleep(10 * time.Second)

	// 再次查询meta
	measurementQueryCondition = NewMeasurementQueryCondition(OP_EQUAL, "CPU")
	queryTimeseriesMetaRequest = NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaRequest.SetCondition(measurementQueryCondition)

	queryTimeseriesMetaResponse, err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()), Equals, 1)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()[0].GetAttributes()), Equals, 1)
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
		timeseriesKey.AddTag("Province", "浙江")
		timeseriesKey.AddTag("City", "杭州")

		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000)
		timeseriesRow.AddField("temperature", NewColumnValue(ColumnType_DOUBLE, 98.5))
		timeseriesRow.AddField("status", NewColumnValue(ColumnType_BOOLEAN, true))
		putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow)
	}
	putTimeseriesDataResponse, err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(putTimeseriesDataResponse.GetFailedRowResults()), Equals, 0)

	time.Sleep(20 * time.Second)

	// 查询meta
	measurementQueryCondition := NewMeasurementQueryCondition(OP_EQUAL, "CPU")
	queryTimeseriesMetaRequest := NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaRequest.SetCondition(measurementQueryCondition)

	queryTimeseriesMetaResponse, err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()), Equals, 100)

	// 删除meta
	deleteTimeseriesMetaRequest := NewDeleteTimeseriesMetaRequest(curTimeseriesTableName)
	for i := 0; i < len(queryTimeseriesMetaResponse.GetTimeseriesMetas()); i++ {
		deleteTimeseriesMetaRequest.AddTimeseriesKeys(queryTimeseriesMetaResponse.GetTimeseriesMetas()[i].GetTimeseriesKey())
	}
	deleteTimeseriesMetaResponse, err := timeseriesClient.DeleteTimeseriesMeta(deleteTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(deleteTimeseriesMetaResponse.GetFailedRowResults()), Equals, 0)

	time.Sleep(20 * time.Second)

	// 再次查询meta
	measurementQueryCondition = NewMeasurementQueryCondition(OP_EQUAL, "CPU")
	queryTimeseriesMetaRequest = NewQueryTimeseriesMetaRequest(curTimeseriesTableName)
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaRequest.SetCondition(measurementQueryCondition)

	queryTimeseriesMetaResponse, err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(queryTimeseriesMetaResponse.GetTimeseriesMetas()), Equals, 0)
}

func (s *TimeseriesSuite) TestCreateAnalyticalStoreWithTimeseriesTable(c *C) {
	// create table with default analytical store
	meta := NewTimeseriesTableMeta("test_analytical_store1")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	// describe table
	describeTimeseriesTableRequest := NewDescribeTimeseriesTableRequset("test_analytical_store1")
	describeTimeseriesTableResponse, err := timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	analyticalStores := describeTimeseriesTableResponse.GetAnalyticalStores()
	c.Assert(len(analyticalStores), Equals, 1)
	c.Assert(analyticalStores[0].StoreName, Equals, "default_analytical_store")
	c.Assert(analyticalStores[0].TimeToLive, DeepEquals, proto.Int32(-1))
	c.Assert(*analyticalStores[0].SyncOption, Equals, SYNC_TYPE_FULL)
	// describe analytical store
	describeAnalyticalStoreRequest := NewDescribeTimeseriesAnalyticalStoreRequest("test_analytical_store1", "default_analytical_store")
	describeAnalyticalStoreResponse, err := timeseriesClient.DescribeTimeseriesAnalyticalStore(describeAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	analyticalStore := describeAnalyticalStoreResponse.AnalyticalStore
	c.Assert(analyticalStore.StoreName, Equals, "default_analytical_store")
	c.Assert(analyticalStore.TimeToLive, DeepEquals, proto.Int32(-1))
	c.Assert(*analyticalStore.SyncOption, Equals, SYNC_TYPE_FULL)
	syncStat := describeAnalyticalStoreResponse.SyncStat
	c.Assert(syncStat.SyncPhase, Equals, SYNC_TYPE_INCR)
	c.Assert(syncStat.CurrentSyncTimestamp, Equals, int64(0))
	storageSize := describeAnalyticalStoreResponse.StorageSize
	c.Assert(storageSize, IsNil)

	// create table without analytical store
	meta = NewTimeseriesTableMeta("test_analytical_store2")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	createTimeseriesTableRequest = NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetEnableAnalyticalStore(false)
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	_, err = timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	// describe table
	describeTimeseriesTableRequest = NewDescribeTimeseriesTableRequset("test_analytical_store2")
	describeTimeseriesTableResponse, err = timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	analyticalStores = describeTimeseriesTableResponse.GetAnalyticalStores()
	c.Assert(len(analyticalStores), Equals, 0)

	// create table with custom analytical store
	syncType := SYNC_TYPE_FULL
	meta = NewTimeseriesTableMeta("test_analytical_store3")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	createTimeseriesTableRequest = NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	createTimeseriesTableRequest.SetAnalyticalStores([]*TimeseriesAnalyticalStore{{
		StoreName:  "custom_analytical_store",
		TimeToLive: proto.Int32(2592000),
		SyncOption: &syncType,
	}})
	_, err = timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	// describe table
	describeTimeseriesTableRequest = NewDescribeTimeseriesTableRequset("test_analytical_store3")
	describeTimeseriesTableResponse, err = timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	analyticalStores = describeTimeseriesTableResponse.GetAnalyticalStores()
	c.Assert(len(analyticalStores), Equals, 1)
	c.Assert(analyticalStores[0].StoreName, Equals, "custom_analytical_store")
	c.Assert(analyticalStores[0].TimeToLive, DeepEquals, proto.Int32(2592000))
	c.Assert(*analyticalStores[0].SyncOption, Equals, SYNC_TYPE_FULL)
}

func (s *TimeseriesSuite) TestCreateAndDeleteAnalyticalStore(c *C) {
	// create table
	meta := NewTimeseriesTableMeta("test_create_and_delete_analytical_store1")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	createTimeseriesTableRequest.SetEnableAnalyticalStore(false)
	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	meta = NewTimeseriesTableMeta("test_create_and_delete_analytical_store2")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	createTimeseriesTableRequest = NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	createTimeseriesTableRequest.SetEnableAnalyticalStore(false)
	_, err = timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)

	// create full sync analytical store
	analyticalStore := NewTimeseriesAnalyticalStore("full_sync_analytical_store")
	analyticalStore.SetSyncOption(SYNC_TYPE_FULL)
	analyticalStore.SetTimeToLive(-1)
	createAnalyticalStoreRequest := NewCreateTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store1", analyticalStore)
	_, err = timeseriesClient.CreateTimeseriesAnalyticalStore(createAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	describeAnalyticalStoreRequest := NewDescribeTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store1", "full_sync_analytical_store")
	describeAnalyticalStoreResponse, err := timeseriesClient.DescribeTimeseriesAnalyticalStore(describeAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	c.Assert(describeAnalyticalStoreResponse.AnalyticalStore.StoreName, Equals, "full_sync_analytical_store")
	c.Assert(describeAnalyticalStoreResponse.AnalyticalStore.TimeToLive, DeepEquals, proto.Int32(-1))
	c.Assert(*describeAnalyticalStoreResponse.AnalyticalStore.SyncOption, Equals, SYNC_TYPE_FULL)
	c.Assert(describeAnalyticalStoreResponse.SyncStat.SyncPhase, Equals, SYNC_TYPE_FULL)
	c.Assert(describeAnalyticalStoreResponse.SyncStat.CurrentSyncTimestamp, Equals, int64(0))
	c.Assert(describeAnalyticalStoreResponse.StorageSize, IsNil)

	// delete analytical store without mapping table
	deleteAnalyticalStoreRequest := NewDeleteTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store1", "full_sync_analytical_store")
	_, err = timeseriesClient.DeleteTimeseriesAnalyticalStore(deleteAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)

	// create incremental sync analytical store
	analyticalStore = NewTimeseriesAnalyticalStore("incr_sync_analytical_store")
	analyticalStore.SetSyncOption(SYNC_TYPE_INCR)
	analyticalStore.SetTimeToLive(2592000)
	createAnalyticalStoreRequest = NewCreateTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store2", analyticalStore)
	_, err = timeseriesClient.CreateTimeseriesAnalyticalStore(createAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	describeAnalyticalStoreRequest = NewDescribeTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store2", "incr_sync_analytical_store")
	describeAnalyticalStoreResponse, err = timeseriesClient.DescribeTimeseriesAnalyticalStore(describeAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	c.Assert(describeAnalyticalStoreResponse.AnalyticalStore.StoreName, Equals, "incr_sync_analytical_store")
	c.Assert(describeAnalyticalStoreResponse.AnalyticalStore.TimeToLive, DeepEquals, proto.Int32(2592000))
	c.Assert(*describeAnalyticalStoreResponse.AnalyticalStore.SyncOption, Equals, SYNC_TYPE_INCR)
	c.Assert(describeAnalyticalStoreResponse.SyncStat.SyncPhase, Equals, SYNC_TYPE_INCR)
	c.Assert(describeAnalyticalStoreResponse.SyncStat.CurrentSyncTimestamp, Equals, int64(0))
	c.Assert(describeAnalyticalStoreResponse.StorageSize, IsNil)

	// create sql mapping table
	createSqlMappingTableRequest := &SQLQueryRequest{Query: "CREATE TABLE `test_create_and_delete_analytical_store2::cpu` (" +
		"`_m_name` varchar(1024) NOT NULL," +
		"`_data_source` varchar(1024) NOT NULL," +
		"`_tags` varchar(1024) NOT NULL," +
		"`_time` bigint(20) NOT NULL," +
		"PRIMARY KEY (`_m_name`,`_data_source`,`_tags`,`_time`)" +
		") ENGINE=AnalyticalStore"}
	_, err = tableStoreClient.SQLQuery(createSqlMappingTableRequest)
	c.Assert(err, Equals, nil)

	// delete analytical store with mapping table
	deleteAnalyticalStoreRequest = NewDeleteTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store2", "incr_sync_analytical_store")
	_, err = timeseriesClient.DeleteTimeseriesAnalyticalStore(deleteAnalyticalStoreRequest)
	c.Assert(err, NotNil)

	// delete analytical store and drop mapping table
	deleteAnalyticalStoreRequest = NewDeleteTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store2", "incr_sync_analytical_store")
	deleteAnalyticalStoreRequest.SetDropMappingTable(true)
	_, err = timeseriesClient.DeleteTimeseriesAnalyticalStore(deleteAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	showTablesResponse, err := tableStoreClient.SQLQuery(&SQLQueryRequest{Query: "SHOW TABLES"})
	c.Assert(err, Equals, nil)
	for showTablesResponse.ResultSet.HasNext() {
		row := showTablesResponse.ResultSet.Next()
		tableName, err := row.GetString(0)
		c.Assert(err, Equals, nil)
		c.Assert(tableName, Not(Equals), "test_create_and_delete_analytical_store2::cpu")
	}
}

func (s *TimeseriesSuite) TestUpdateAnalyticalStore(c *C) {
	// create table
	meta := NewTimeseriesTableMeta("test_update_analytical_store")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	describeAnalyticalStoreRequest := NewDescribeTimeseriesAnalyticalStoreRequest("test_update_analytical_store", "default_analytical_store")
	describeAnalyticalStoreResponse, err := timeseriesClient.DescribeTimeseriesAnalyticalStore(describeAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	c.Assert(describeAnalyticalStoreResponse.AnalyticalStore.StoreName, Equals, "default_analytical_store")
	c.Assert(describeAnalyticalStoreResponse.AnalyticalStore.TimeToLive, DeepEquals, proto.Int32(-1))

	// update analytical store
	analyticalStore := NewTimeseriesAnalyticalStore("default_analytical_store")
	analyticalStore.SetTimeToLive(2592000)
	updateAnalyticalStoreRequest := NewUpdateTimeseriesAnalyticalStoreRequest("test_update_analytical_store", analyticalStore)
	_, err = timeseriesClient.UpdateTimeseriesAnalyticalStore(updateAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	describeAnalyticalStoreRequest = NewDescribeTimeseriesAnalyticalStoreRequest("test_update_analytical_store", "default_analytical_store")
	describeAnalyticalStoreResponse, err = timeseriesClient.DescribeTimeseriesAnalyticalStore(describeAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)
	c.Assert(describeAnalyticalStoreResponse.AnalyticalStore.StoreName, Equals, "default_analytical_store")
	c.Assert(describeAnalyticalStoreResponse.AnalyticalStore.TimeToLive, DeepEquals, proto.Int32(2592000))
}

func (s *TimeseriesSuite) TestDescribeIndexSyncPhase(c *C) {
	// create table
	meta := NewTimeseriesTableMeta("test_describe_index_sync_phase")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)

	// describe table
	describeTableRequest := &DescribeTableRequest{TableName: "test_describe_index_sync_phase#timeseries"}
	describeTableResponse, err := tableStoreClient.DescribeTable(describeTableRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(describeTableResponse.IndexMetas), Equals, 1)
	c.Assert(*describeTableResponse.IndexMetas[0].IndexSyncPhase, Equals, SyncPhase_INCR)

	// delete default analytical store
	deleteAnalyticalStoreRequest := NewDeleteTimeseriesAnalyticalStoreRequest("test_describe_index_sync_phase", "default_analytical_store")
	_, err = timeseriesClient.DeleteTimeseriesAnalyticalStore(deleteAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)

	// create full sync analytical store
	analyticalStore := NewTimeseriesAnalyticalStore("full_sync_analytical_store")
	analyticalStore.SetSyncOption(SYNC_TYPE_FULL)
	analyticalStore.SetTimeToLive(-1)
	createAnalyticalStoreRequest := NewCreateTimeseriesAnalyticalStoreRequest("test_describe_index_sync_phase", analyticalStore)
	_, err = timeseriesClient.CreateTimeseriesAnalyticalStore(createAnalyticalStoreRequest)
	c.Assert(err, Equals, nil)

	// describe table
	describeTableRequest = &DescribeTableRequest{TableName: "test_describe_index_sync_phase#timeseries"}
	describeTableResponse, err = tableStoreClient.DescribeTable(describeTableRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(describeTableResponse.IndexMetas), Equals, 1)
	c.Assert(*describeTableResponse.IndexMetas[0].IndexSyncPhase, Equals, SyncPhase_FULL)
}

func (s *TimeseriesSuite) TestCustomPrimaryKeys(c *C) {
	// create table
	meta := NewTimeseriesTableMeta("test_custom_primary_keys")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	meta.AddTimeseriesKey("_m_name")
	meta.AddTimeseriesKey("region")
	meta.AddTimeseriesKey("_tags")
	meta.AddFieldPrimaryKey("cores", PrimaryKeyType_INTEGER)
	meta.AddFieldPrimaryKey("frequency", PrimaryKeyType_STRING)
	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)

	// describe table
	describeTableRequest := NewDescribeTimeseriesTableRequset("test_custom_primary_keys")
	describeTableResponse, err := timeseriesClient.DescribeTimeseriesTable(describeTableRequest)
	c.Assert(err, Equals, nil)
	pks := describeTableResponse.GetTimeseriesTableMeta().timeseriesKeys
	c.Assert(len(pks), Equals, 3)
	c.Assert(pks[0], Equals, "_m_name")
	c.Assert(pks[1], Equals, "region")
	c.Assert(pks[2], Equals, "_tags")
	pkFields := describeTableResponse.GetTimeseriesTableMeta().fieldPrimaryKeys
	c.Assert(len(pkFields), Equals, 2)
	c.Assert(*pkFields[0].Name, Equals, "cores")
	c.Assert(*pkFields[0].Type, Equals, PrimaryKeyType_INTEGER)
	c.Assert(*pkFields[1].Name, Equals, "frequency")
	c.Assert(*pkFields[1].Type, Equals, PrimaryKeyType_STRING)

	// list table
	listTableResponse, err := timeseriesClient.ListTimeseriesTable()
	c.Assert(err, Equals, nil)
	hasTestPrimaryKeyFieldsTable := false
	for _, meta = range listTableResponse.GetTimeseriesTableMeta() {
		if meta.GetTimeseriesTableName() == "test_custom_primary_keys" {
			hasTestPrimaryKeyFieldsTable = true
			pks = meta.timeseriesKeys
			c.Assert(len(pks), Equals, 3)
			c.Assert(pks[0], Equals, "_m_name")
			c.Assert(pks[1], Equals, "region")
			c.Assert(pks[2], Equals, "_tags")
			pkFields = meta.fieldPrimaryKeys
			c.Assert(len(pkFields), Equals, 2)
			c.Assert(*pkFields[0].Name, Equals, "cores")
			c.Assert(*pkFields[0].Type, Equals, PrimaryKeyType_INTEGER)
			c.Assert(*pkFields[1].Name, Equals, "frequency")
			c.Assert(*pkFields[1].Type, Equals, PrimaryKeyType_STRING)
			break
		}
	}
	c.Assert(hasTestPrimaryKeyFieldsTable, Equals, true)

	// put data
	putTimeseriesDataRequest := NewPutTimeseriesDataRequest("test_custom_primary_keys")
	timeseriesKey := NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("cpu")
	timeseriesKey.AddTag("region", "hangzhou")
	timeseriesKey.AddTag("vendor", "intel")
	for i := 0; i < 10; i++ {
		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano() / 1000)
		timeseriesRow.AddField("cores", NewColumnValue(ColumnType_INTEGER, 8))
		timeseriesRow.AddField("frequency", NewColumnValue(ColumnType_STRING, fmt.Sprintf("%dGHz", i)))
		timeseriesRow.AddField("temperature", NewColumnValue(ColumnType_DOUBLE, 98.5))
		putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow)
	}
	putTimeseriesDataResponse, err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(putTimeseriesDataResponse.GetFailedRowResults()), Equals, 0)

	// get data
	getTimeseriesDataRequest := NewGetTimeseriesDataRequest("test_custom_primary_keys")
	getTimeseriesDataRequest.SetTimeseriesKey(timeseriesKey)
	getTimeseriesDataRequest.SetTimeRange(0, time.Now().UnixNano()/1000)
	getTimeseriesDataResponse, err := timeseriesClient.GetTimeseriesData(getTimeseriesDataRequest)
	c.Assert(err, Equals, nil)
	rows := getTimeseriesDataResponse.GetRows()
	c.Assert(len(rows), Equals, 10)
	for i := 0; i < 10; i++ {
		pk := rows[i].GetTimeseriesKey()
		c.Assert(pk.GetMeasurementName(), Equals, "cpu")
		c.Assert(pk.GetDataSource(), Equals, "")
		tags := pk.GetTags()
		c.Assert(len(tags), Equals, 2)
		c.Assert(tags["region"], Equals, "hangzhou")
		c.Assert(tags["vendor"], Equals, "intel")
		fields := rows[i].GetFieldsMap()
		c.Assert(len(fields), Equals, 3)
		c.Assert(fields["cores"].Value, Equals, int64(8))
		c.Assert(fields["frequency"].Value, Equals, fmt.Sprintf("%dGHz", i))
		c.Assert(fields["temperature"].Value, Equals, 98.5)
	}
}

func (s *TimeseriesSuite) TestCustomPrimaryKeysMeta(c *C) {
	// create table
	meta := NewTimeseriesTableMeta("test_custom_primary_keys_meta")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	meta.AddTimeseriesKey("_m_name")
	meta.AddTimeseriesKey("region")
	meta.AddTimeseriesKey("_tags")
	meta.AddFieldPrimaryKey("cores", PrimaryKeyType_INTEGER)
	meta.AddFieldPrimaryKey("frequency", PrimaryKeyType_STRING)
	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)

	// update meta
	timeseriesKey := NewTimeseriesKey()
	timeseriesKey.SetMeasurementName("cpu")
	timeseriesKey.AddTag("region", "hangzhou")
	timeseriesKey.AddTag("vendor", "intel")
	timeseriesMeta := NewTimeseriesMeta(timeseriesKey)
	updateTimeseriesMetaRequest := NewUpdateTimeseriesMetaRequest("test_custom_primary_keys_meta")
	updateTimeseriesMetaRequest.AddTimeseriesMetas(timeseriesMeta)
	updateTimeseriesMetaResponse, err := timeseriesClient.UpdateTimeseriesMeta(updateTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(updateTimeseriesMetaResponse.GetFailedRowResults()), Equals, 0)

	// wait for meta sync
	time.Sleep(30 * time.Second)

	// query meta
	queryTimeseriesMetaRequest := NewQueryTimeseriesMetaRequest("test_custom_primary_keys_meta")
	queryTimeseriesMetaRequest.SetLimit(-1)
	queryTimeseriesMetaResponse, err := timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	metas := queryTimeseriesMetaResponse.GetTimeseriesMetas()
	c.Assert(len(metas), Equals, 1)
	c.Assert(metas[0].GetTimeseriesKey().GetMeasurementName(), Equals, "cpu")
	c.Assert(metas[0].GetTimeseriesKey().GetDataSource(), Equals, "")
	tags := metas[0].GetTimeseriesKey().GetTags()
	c.Assert(len(tags), Equals, 2)
	c.Assert(tags["region"], Equals, "hangzhou")
	c.Assert(tags["vendor"], Equals, "intel")

	// delete meta
	deleteTimeseriesMetaRequest := NewDeleteTimeseriesMetaRequest("test_custom_primary_keys_meta")
	deleteTimeseriesMetaRequest.AddTimeseriesKeys(timeseriesKey)
	deleteTimeseriesMetaResponse, err := timeseriesClient.DeleteTimeseriesMeta(deleteTimeseriesMetaRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(deleteTimeseriesMetaResponse.GetFailedRowResults()), Equals, 0)
	for i := 0; i < 5; i++ { // wait for meta sync
		time.Sleep(30 * time.Second)
		queryTimeseriesMetaResponse, err = timeseriesClient.QueryTimeseriesMeta(queryTimeseriesMetaRequest)
		c.Assert(err, Equals, nil)
		metas = queryTimeseriesMetaResponse.GetTimeseriesMetas()
		if len(metas) == 0 {
			break
		}
	}
	c.Assert(len(metas), Equals, 0)
}

func (s *TimeseriesSuite) TestCustomPrimaryKeysTags(c *C) {
	// create table
	meta := NewTimeseriesTableMeta("test_custom_primary_keys_tags")
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	meta.AddTimeseriesKey("measurement")
	meta.AddTimeseriesKey("vendor")
	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)

	// put data
	putTimeseriesDataRequest := NewPutTimeseriesDataRequest("test_custom_primary_keys_tags")
	timeseriesKey := NewTimeseriesKey()
	timeseriesKey.AddTag("measurement", "cpu")
	timeseriesKey.AddTag("vendor", "intel")
	for i := 0; i < 10; i++ {
		timeseriesRow := NewTimeseriesRow(timeseriesKey)
		timeseriesRow.SetTimeInus(time.Now().UnixNano()/1000 + int64(i))
		timeseriesRow.AddField("temperature", NewColumnValue(ColumnType_DOUBLE, 98.5))
		putTimeseriesDataRequest.AddTimeseriesRows(timeseriesRow)
	}
	putTimeseriesDataResponse, err := timeseriesClient.PutTimeseriesData(putTimeseriesDataRequest)
	c.Assert(err, Equals, nil)
	c.Assert(len(putTimeseriesDataResponse.GetFailedRowResults()), Equals, 0)

	// get data
	getTimeseriesDataRequest := NewGetTimeseriesDataRequest("test_custom_primary_keys_tags")
	getTimeseriesDataRequest.SetTimeseriesKey(timeseriesKey)
	getTimeseriesDataRequest.SetTimeRange(0, time.Now().UnixNano()/1000)
	getTimeseriesDataResponse, err := timeseriesClient.GetTimeseriesData(getTimeseriesDataRequest)
	c.Assert(err, Equals, nil)
	rows := getTimeseriesDataResponse.GetRows()
	c.Assert(len(rows), Equals, 10)
	for i := 0; i < 10; i++ {
		pk := rows[i].GetTimeseriesKey()
		tags := pk.GetTags()
		c.Assert(len(tags), Equals, 2)
		c.Assert(tags["measurement"], Equals, "cpu")
		c.Assert(tags["vendor"], Equals, "intel")
		fields := rows[i].GetFieldsMap()
		c.Assert(len(fields), Equals, 1)
		c.Assert(fields["temperature"].Value, Equals, 98.5)
	}
}

func (s *TimeseriesSuite) TestLastpointIndexCreateAndDelete(c *C) {
	tableName := "test_lastpoint_index_create_and_delete"
	indexName := "test_lastpoint_index_index_table"
	s.deleteTimeseriesTable(tableName)

	// create table with index
	meta := NewTimeseriesTableMeta(tableName)
	meta.SetTimeseriesTableOptions(&TimeseriesTableOptions{-1})
	createTimeseriesTableRequest := NewCreateTimeseriesTableRequest()
	createTimeseriesTableRequest.SetTimeseriesTableMeta(meta)
	createTimeseriesTableRequest.SetLastpointIndexNames([]string{indexName})
	_, err := timeseriesClient.CreateTimeseriesTable(createTimeseriesTableRequest)
	c.Assert(err, Equals, nil)

	// describe timeseries table
	describeTimeseriesTableRequest := NewDescribeTimeseriesTableRequset(tableName)
	describeTimeseriesTableResponse, err := timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	c.Assert(describeTimeseriesTableResponse.GetLastpointIndexNames(), DeepEquals, []string{indexName})

	// describe index table
	describeTableRequest := &DescribeTableRequest{
		TableName: indexName,
	}
	describeTableResponse, err := tableStoreClient.DescribeTable(describeTableRequest)
	c.Assert(err, Equals, nil)
	c.Assert(describeTableResponse.TableMeta.TableName, Equals, indexName)
	c.Assert(len(describeTableResponse.TableMeta.SchemaEntry), Equals, 4)

	// delete index
	deleteLastpointIndexRequest := NewDeleteTimeseriesLastpointIndexRequest(tableName, indexName)
	c.Assert(deleteLastpointIndexRequest.GetTimeseriesTableName(), Equals, tableName)
	c.Assert(deleteLastpointIndexRequest.GetLastpointIndexTableName(), Equals, indexName)
	_, err = timeseriesClient.DeleteTimeseriesLastpointIndex(deleteLastpointIndexRequest)
	c.Assert(err, Equals, nil)

	// describe timeseries table
	describeTimeseriesTableResponse, err = timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	c.Assert(describeTimeseriesTableResponse.GetLastpointIndexNames(), DeepEquals, []string(nil))

	// create index table
	createLastpointIndexRequest := NewCreateTimeseriesLastpointIndexRequest(tableName, indexName, true)
	c.Assert(createLastpointIndexRequest.GetTimeseriesTableName(), Equals, tableName)
	c.Assert(createLastpointIndexRequest.GetLastpointIndexTableName(), Equals, indexName)
	_, err = timeseriesClient.CreateTimeseriesLastpointIndex(createLastpointIndexRequest)
	c.Assert(err, Equals, nil)

	// describe timeseries table
	describeTimeseriesTableResponse, err = timeseriesClient.DescribeTimeseriesTable(describeTimeseriesTableRequest)
	c.Assert(err, Equals, nil)
	c.Assert(describeTimeseriesTableResponse.GetLastpointIndexNames(), DeepEquals, []string{indexName})
}
