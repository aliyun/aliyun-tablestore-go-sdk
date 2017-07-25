package tablestore

import (
	"testing"
	. "gopkg.in/check.v1"
	"fmt"
	"os"
	"strconv"
	"strings"
	"runtime"
	"time"
	"math/rand"
	"net/http"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/tsprotocol"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type TableStoreSuite struct{}

var tableNamePrefix string

var _ = Suite(&TableStoreSuite{})

var defaultTableName = "defaulttable"
var rangeQueryTableName = "rangetable"

// Todo: use config
var client TableStoreApi
var invalidClient TableStoreApi

func (s *TableStoreSuite) SetUpSuite(c *C) {

	endpoint := os.Getenv("OTS_TEST_ENDPOINT")
	instanceName := os.Getenv("OTS_TEST_INSTANCENAME")
	accessKeyId := os.Getenv("OTS_TEST_KEYID")
	accessKeySecret := os.Getenv("OTS_TEST_SECRET")
	client = NewClient(endpoint, instanceName, accessKeyId, accessKeySecret)

	tableNamePrefix = strings.Replace(runtime.Version(), ".", "", -1)
	defaultTableName = tableNamePrefix + defaultTableName
	rangeQueryTableName =  tableNamePrefix + rangeQueryTableName
	PrepareTable(defaultTableName)
	PrepareTable2(rangeQueryTableName)
	invalidClient = NewClient(endpoint, instanceName, accessKeyId, "invalidsecret")
}

func PrepareTable(tableName string) error {
	createtableRequest := new(CreateTableRequest)
	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput
	_, error := client.CreateTable(createtableRequest)
	return error
}

func PrepareTable2(tableName string) error {
	createtableRequest := new(CreateTableRequest)
	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk2", PrimaryKeyType_STRING)
	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput
	_, error := client.CreateTable(createtableRequest)
	return error
}

func (s *TableStoreSuite) TestCreateTable(c *C) {
	fmt.Println("TestCreateTable finished")

	tableName := tableNamePrefix + "testcreatetable1"

	deleteReq := new(DeleteTableRequest)
	deleteReq.TableName = tableName
	client.DeleteTable(deleteReq)

	createtableRequest := new(CreateTableRequest)

	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)

	tableOption := new(TableOption)

	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3

	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0

	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput

	_, error := client.CreateTable(createtableRequest)
	c.Check(error, Equals, nil)

	fmt.Println("TestCreateTable finished")
}

func (s *TableStoreSuite) TestReCreateTableAndPutRow(c *C) {
	fmt.Println("TestReCreateTableAndPutRow started")

	tableName := tableNamePrefix + "testrecreatetable1"

	deleteReq := new(DeleteTableRequest)
	deleteReq.TableName = tableName
	client.DeleteTable(deleteReq)

	createtableRequest := new(CreateTableRequest)

	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)

	tableOption := new(TableOption)

	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3

	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0

	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput

	_, error := client.CreateTable(createtableRequest)
	c.Check(error, Equals, nil)

	//time.Sleep(500 * time.Millisecond)
	_, error = client.DeleteTable(deleteReq)
	c.Check(error, Equals, nil)

	_, error = client.CreateTable(createtableRequest)
	c.Check(error, Equals, nil)

	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = tableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", "key1")
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", "col1data1")
	putRowChange.AddColumn("col2", int64(100))
	putRowChange.AddColumn("col3", float64(2.1))
	putRowChange.AddColumn("col4", true)
	putRowChange.AddColumn("col5", int64(50))
	putRowChange.AddColumn("col6", int64(60))
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error = client.PutRow(putRowRequest)
	c.Check(error, Equals, nil)

	fmt.Println("TestReCreateTableAndPutRow finished")
}

func (s *TableStoreSuite) TestListTable(c *C) {
	listtables, error := client.ListTable()
	c.Check(error, Equals, nil)
	defaultTableExist := false
	for _, table := range (listtables.TableNames) {
		fmt.Println(table)
		if table == defaultTableName {
			defaultTableExist = true
			break
		}
	}

	c.Check(defaultTableExist, Equals, true)
}

func (s *TableStoreSuite) TestUpdateAndDescribeTable(c *C) {
	fmt.Println("TestUpdateAndDescribeTable started")
	updateTableReq := new(UpdateTableRequest)
	updateTableReq.TableName = defaultTableName
	updateTableReq.TableOption = new(TableOption)
	updateTableReq.TableOption.TimeToAlive = -1
	updateTableReq.TableOption.MaxVersion = 5

	updateTableResp, error := client.UpdateTable(updateTableReq)
	c.Assert(error, Equals, nil)
	c.Assert(updateTableResp, NotNil)
	c.Assert(updateTableResp.TableOption.TimeToAlive, Equals, updateTableReq.TableOption.TimeToAlive)
	c.Assert(updateTableResp.TableOption.MaxVersion, Equals, updateTableReq.TableOption.MaxVersion)

	describeTableReq := new(DescribeTableRequest)
	describeTableReq.TableName = defaultTableName
	describ, error := client.DescribeTable(describeTableReq)
	c.Assert(error, Equals, nil)

	c.Assert(describ, NotNil)
	c.Assert(describ.TableOption.TimeToAlive, Equals, updateTableReq.TableOption.TimeToAlive)
	c.Assert(describ.TableOption.MaxVersion, Equals, updateTableReq.TableOption.MaxVersion)
	fmt.Println("TestUpdateAndDescribeTable finished")
}

func (s *TableStoreSuite) TestTableWithKeyAutoIncrement(c *C) {
	tableName := tableNamePrefix + "incrementtable"
	createtableRequest := new(CreateTableRequest)

	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumnOption("pk2", PrimaryKeyType_INTEGER, AUTO_INCREMENT)

	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3

	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0

	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput

	client.CreateTable(createtableRequest)
	rowCount := 100
	for i := 0; i < rowCount; i++ {
		putRowRequest := new(PutRowRequest)
		putRowChange := new(PutRowChange)
		putRowChange.TableName = tableName
		putPk := new(PrimaryKey)
		putPk.AddPrimaryKeyColumn("pk1", "key" + strconv.Itoa(i))
		putPk.AddPrimaryKeyColumnWithAutoIncrement("pk2")
		putRowChange.PrimaryKey = putPk
		putRowChange.AddColumn("col1", "col1data1")
		putRowChange.AddColumn("col2", int64(100))
		putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
		putRowRequest.PutRowChange = putRowChange
		putRowRequest.PutRowChange.SetReturnPk()
		response, error := client.PutRow(putRowRequest)
		c.Check(error, Equals, nil)
		c.Check(len(response.PrimaryKey.PrimaryKeys), Equals, 2)
		c.Check(response.PrimaryKey.PrimaryKeys[0].ColumnName, Equals, "pk1")
		c.Check(response.PrimaryKey.PrimaryKeys[0].Value, Equals,  "key" + strconv.Itoa(i))
		c.Check(response.PrimaryKey.PrimaryKeys[1].ColumnName, Equals, "pk2")
		c.Check(response.PrimaryKey.PrimaryKeys[1].Value.(int64) > 0, Equals, true)

		fmt.Println(response.PrimaryKey.PrimaryKeys[1].Value)
	}

	describeTableReq := new(DescribeTableRequest)
	describeTableReq.TableName = tableName
	_, error := client.DescribeTable(describeTableReq)
	c.Check(error, IsNil)
}

func (s *TableStoreSuite) TestPutGetRow(c *C) {
	fmt.Println("TestPutGetRow started")
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", "Key6")
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", "col1data1")
	putRowChange.AddColumn("col2", int64(100))
	putRowChange.AddColumn("col3", float64(2.1))
	putRowChange.AddColumn("col4", true)
	putRowChange.AddColumn("col5", int64(50))
	putRowChange.AddColumn("col6", int64(60))
	putRowChange.AddColumn("col7", []byte("testbytes"))
	putRowChange.AddColumn("col8", false)
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	c.Check(error, Equals, nil)

	getRowRequest := new(GetRowRequest)
	criteria := new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	getResp, error := client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp, NotNil)
	c.Check(len(getResp.PrimaryKey.PrimaryKeys), Equals, 1)
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].ColumnName, Equals, "pk1")
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].Value, Equals, "Key6")
	c.Check(len(getResp.Columns), Equals, 8)
	c.Check(getResp.Columns[0].ColumnName, Equals, "col1")
	c.Check(getResp.Columns[0].Value, Equals, "col1data1")
	c.Check(getResp.Columns[1].ColumnName, Equals, "col2")
	c.Check(getResp.Columns[1].Value, Equals, int64(100))
	c.Check(getResp.Columns[2].ColumnName, Equals, "col3")
	c.Check(getResp.Columns[2].Value, Equals, float64(2.1))
	c.Check(getResp.Columns[3].ColumnName, Equals, "col4")
	c.Check(getResp.Columns[3].Value, Equals, true)
	c.Check(getResp.Columns[4].ColumnName, Equals, "col5")
	c.Check(getResp.Columns[4].Value, Equals, int64(50))
	c.Check(getResp.Columns[5].ColumnName, Equals, "col6")
	c.Check(getResp.Columns[5].Value, Equals, int64(60))
	c.Check(getResp.Columns[6].ColumnName, Equals, "col7")
	mapData := getResp.GetColumnMap()
	c.Check(mapData.Columns["col1"][0].Value, Equals, "col1data1")
	c.Check(mapData.Columns["col2"][0].Value, Equals, int64(100))
	c.Check(mapData.Columns["col3"][0].Value, Equals, float64(2.1))
	c.Check(mapData.Columns["col4"][0].Value, Equals, true)
	c.Check(mapData.Columns["col5"][0].Value, Equals, int64(50))
	c.Check(mapData.Columns["col6"][0].Value, Equals, int64(60))

	sortedColumn, error := mapData.GetRange(2,2)
	c.Check(error, Equals, nil)
	c.Check(len(sortedColumn), Equals, 2)
	c.Check(sortedColumn[0], Equals, mapData.Columns["col3"][0])
	c.Check(sortedColumn[1], Equals, mapData.Columns["col4"][0])

	mapData2 := getResp.GetColumnMap()
	c.Check(mapData2.Columns["col1"][0].Value, Equals, "col1data1")

	_, error = mapData.GetRange(2,10)
	c.Check(error, NotNil)
	// Test add column to get
	getRowRequest = new(GetRowRequest)
	criteria = new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col2")

	getResp, error = client.GetRow(getRowRequest)

	c.Check(error, Equals, nil)
	c.Check(getResp, NotNil)
	c.Check(len(getResp.Columns), Equals, 2)

	_, error = invalidClient.GetRow(getRowRequest)
	c.Check(error, NotNil)

	getRowRequest = new(GetRowRequest)
	criteria = new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	_, error = client.GetRow(getRowRequest)
	c.Check(error, NotNil)

	notExistPk := new(PrimaryKey)
	notExistPk.AddPrimaryKeyColumn("pk1", "notexistpk")
	getRowRequest = new(GetRowRequest)
	criteria = new(SingleRowQueryCriteria);

	criteria.PrimaryKey = notExistPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1

	getResp, error = client.GetRow(getRowRequest)
	c.Check(error, IsNil)
	c.Check(getResp, NotNil)

	colmap := getResp.GetColumnMap()
	c.Check(colmap, NotNil)

	fmt.Println("TestPutGetRow finished")
}

func (s *TableStoreSuite) TestCreateTableAndPutRow(c *C) {
	fmt.Println("TestCreateTableAndPutRow finished")

	tableName := tableNamePrefix + "testpkschema"
	deleteReq := new(DeleteTableRequest)
	deleteReq.TableName = tableName
	client.DeleteTable(deleteReq)

	createtableRequest := new(CreateTableRequest)

	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk2", PrimaryKeyType_INTEGER)
	tableMeta.AddPrimaryKeyColumn("pk3", PrimaryKeyType_BINARY)

	tableOption := new(TableOption)

	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3

	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0

	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput

	_, error := client.CreateTable(createtableRequest)
	c.Check(error, Equals, nil)

	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = tableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", "key2")
	putPk.AddPrimaryKeyColumn("pk2", int64(5))
	putPk.AddPrimaryKeyColumn("pk3", []byte("byteskey1"))
	putRowChange.PrimaryKey = putPk

	timeNow := time.Now().Unix() * 1000
	putRowChange.AddColumnWithTimestamp("col1", "col1data1", timeNow)
	putRowChange.AddColumn("col2", int64(100))
	putRowChange.AddColumn("col3", float64(2.1))
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error = client.PutRow(putRowRequest)
	c.Check(error, Equals, nil)

	fmt.Println("TestCreateTableAndPutRow finished")
}

func (s *TableStoreSuite) TestPutGetRowWithTimestamp(c *C) {
	fmt.Println("TestPutGetRowWithTimestamp started")
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", "testtskey1")
	putRowChange.PrimaryKey = putPk
	timeNow :=time.Now().Unix() * 1000
	putRowChange.AddColumnWithTimestamp("col1", "col1data1", timeNow)
	putRowChange.AddColumn("col2", int64(100))
	putRowChange.AddColumn("col3", float64(2.1))
	putRowChange.AddColumn("col4", true)
	putRowChange.AddColumn("col5", int64(50))
	putRowChange.AddColumn("col6", int64(60))
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	c.Check(error, Equals, nil)

	getRowRequest := new(GetRowRequest)
	criteria := new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	// getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Specific: timeNow}
	getResp, error := client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp, NotNil)
	c.Check(len(getResp.PrimaryKey.PrimaryKeys), Equals, 1)
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].ColumnName, Equals, "pk1")
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].Value, Equals, "testtskey1")
	c.Check(len(getResp.Columns), Equals, 6)
	c.Check(getResp.Columns[0].ColumnName, Equals, "col1")
	c.Check(getResp.Columns[0].Value, Equals, "col1data1")
	c.Check(getResp.Columns[0].Timestamp, Equals, timeNow)
	c.Check(getResp.Columns[1].ColumnName, Equals, "col2")
	c.Check(getResp.Columns[1].Value, Equals, int64(100))
	c.Check(getResp.Columns[2].ColumnName, Equals, "col3")
	c.Check(getResp.Columns[2].Value, Equals, float64(2.1))
	c.Check(getResp.Columns[3].ColumnName, Equals, "col4")
	c.Check(getResp.Columns[3].Value, Equals, true)
	c.Check(getResp.Columns[4].ColumnName, Equals, "col5")
	c.Check(getResp.Columns[4].Value, Equals, int64(50))
	c.Check(getResp.Columns[5].ColumnName, Equals, "col6")
	c.Check(getResp.Columns[5].Value, Equals, int64(60))

	getRowRequest.SingleRowQueryCriteria.MaxVersion = 0
	fmt.Println("timerange", timeNow)
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Specific: timeNow - 1}
	getResp2, error := client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp2, NotNil)
	c.Check(len(getResp2.PrimaryKey.PrimaryKeys), Equals, 0)

	getRowRequest.SingleRowQueryCriteria.MaxVersion = 0
	fmt.Println("timerange", timeNow)
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Start: timeNow + 1, End: timeNow + 2}
	getResp2, error = client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp2, NotNil)

	getRowRequest.SingleRowQueryCriteria.MaxVersion = 0
	fmt.Println("timerange", timeNow)
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Specific: timeNow - 1}
	getResp2, error = client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp2, NotNil)
	c.Check(len(getResp2.PrimaryKey.PrimaryKeys), Equals, 0)

	fmt.Println("timerange", timeNow)
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Start: timeNow - 1, End: timeNow + 2}
	getResp2, error = client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp2, NotNil)
	c.Check(len(getResp2.PrimaryKey.PrimaryKeys), Equals, 1)

	fmt.Println("TestPutGetRowWithTimestamp finished")
}

func (s *TableStoreSuite) TestPutGetRowWithFilter(c *C) {
	fmt.Println("TestPutGetRowWithFilter started")
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", "Key6")
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", "col1data1")
	putRowChange.AddColumn("col2", int64(100))
	putRowChange.AddColumn("col3", float64(5.1))
	putRowChange.AddColumn("col4", true)
	putRowChange.AddColumn("col5", int64(50))
	putRowChange.AddColumn("col6", int64(60))
	putRowChange.AddColumn("col7", []byte("testbytes"))
	putRowChange.AddColumn("col8", false)
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	clCondition1 := NewSingleColumnCondition("col2", CT_GREATER_EQUAL, int64(100))
	clCondition2 := NewSingleColumnCondition("col5", CT_NOT_EQUAL, int64(20))
	clCondition3 := NewSingleColumnCondition("col6", CT_LESS_THAN, int64(100))
	clCondition4 := NewSingleColumnCondition("col4", CT_EQUAL, true)
	clCondition5 := NewSingleColumnCondition("col1", CT_EQUAL, "col1data1")
	clCondition6 := NewSingleColumnCondition("col3", CT_LESS_EQUAL, float64(5.1))
	clCondition7 := NewSingleColumnCondition("col7", CT_EQUAL, []byte("testbytes"))
	clCondition8 := NewSingleColumnCondition("col5", CT_GREATER_THAN, int64(20))

	cf := NewCompositeColumnCondition(LO_AND)
	cf.AddFilter(clCondition1)
	cf.AddFilter(clCondition2)
	cf.AddFilter(clCondition3)
	cf.AddFilter(clCondition4)
	cf.AddFilter(clCondition5)
	cf.AddFilter(clCondition6)
	cf.AddFilter(clCondition7)
	cf.AddFilter(clCondition8)
	putRowChange.SetColumnCondition(cf)

	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	c.Check(error, Equals, nil)

	cf2 := NewCompositeColumnCondition(LO_OR)
	cf2.AddFilter(clCondition7)
	cf2.AddFilter(clCondition8)
	cf3 := NewCompositeColumnCondition(LO_NOT)
	clCondition9 := NewSingleColumnCondition("col5", CT_GREATER_THAN, int64(200))
	cf3.AddFilter(clCondition9)
	cf2.AddFilter(cf3)

	getRowRequest := new(GetRowRequest)
	criteria := new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	getRowRequest.SingleRowQueryCriteria.SetFilter(cf2)
	getResp, error := client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp, NotNil)
	c.Check(len(getResp.PrimaryKey.PrimaryKeys), Equals, 1)
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].ColumnName, Equals, "pk1")
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].Value, Equals, "Key6")
	c.Check(len(getResp.Columns), Equals, 8)
	c.Check(getResp.Columns[0].ColumnName, Equals, "col1")
	c.Check(getResp.Columns[0].Value, Equals, "col1data1")
	c.Check(getResp.Columns[1].ColumnName, Equals, "col2")
	c.Check(getResp.Columns[1].Value, Equals, int64(100))
	c.Check(getResp.Columns[2].ColumnName, Equals, "col3")
	c.Check(getResp.Columns[2].Value, Equals, float64(5.1))
	c.Check(getResp.Columns[3].ColumnName, Equals, "col4")
	c.Check(getResp.Columns[3].Value, Equals, true)
	c.Check(getResp.Columns[4].ColumnName, Equals, "col5")
	c.Check(getResp.Columns[4].Value, Equals, int64(50))
	c.Check(getResp.Columns[5].ColumnName, Equals, "col6")
	c.Check(getResp.Columns[5].Value, Equals, int64(60))

	getRowRequest = new(GetRowRequest)
	criteria = new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1

	pagedFilter := &PaginationFilter{}
	pagedFilter.Limit = 3
	pagedFilter.Offset = 1
	getRowRequest.SingleRowQueryCriteria.SetFilter(pagedFilter)
	getResp, error = client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp, NotNil)
	c.Check(len(getResp.Columns), Equals, 3)


	getRowRequest = new(GetRowRequest)
	criteria = new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1

	getRowRequest.SingleRowQueryCriteria.SetStartColumn("col3")
	pagedFilter = &PaginationFilter{}
	pagedFilter.Limit = 3
	pagedFilter.Offset = 1
	getRowRequest.SingleRowQueryCriteria.SetFilter(pagedFilter)
	getResp, error = client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp, NotNil)
	c.Check(getResp.Columns[0].ColumnName, Equals, "col4")
	fmt.Println("TestPutGetRowWithFilter finished")
}

func (s *TableStoreSuite) TestPutUpdateDeleteRow(c *C) {
	fmt.Println("TestPutUpdateDeleteRow started")
	keyToUpdate := "pk1toupdate"
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", keyToUpdate)
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", "col1data1")
	timeNow := int64(time.Now().Unix() * 1000)
	putRowChange.AddColumnWithTimestamp("col10", "col10data10", timeNow)
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	c.Check(error, Equals, nil)

	updateRowRequest := new(UpdateRowRequest)
	updateRowChange := new(UpdateRowChange)
	updateRowChange.TableName = defaultTableName
	updatePk := new(PrimaryKey)
	updatePk.AddPrimaryKeyColumn("pk1", keyToUpdate)
	updateRowChange.PrimaryKey = updatePk
	updateRowChange.DeleteColumn("col1")
	updateRowChange.DeleteColumnWithTimestamp("col10", timeNow)
	updateRowChange.PutColumn("col2", int64(77))
	updateRowChange.PutColumn("col3", "newcol3")
	updateRowChange.SetCondition(RowExistenceExpectation_EXPECT_EXIST)
	updateRowRequest.UpdateRowChange = updateRowChange
	_, error = client.UpdateRow(updateRowRequest)
	c.Check(error, Equals, nil)

	getRowRequest := new(GetRowRequest)
	criteria := new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	getResp, error := client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp, NotNil)

	c.Check(len(getResp.PrimaryKey.PrimaryKeys), Equals, 1)
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].ColumnName, Equals, "pk1")
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].Value, Equals, keyToUpdate)
	c.Check(len(getResp.Columns), Equals, 2)
	c.Check(getResp.Columns[0].ColumnName, Equals, "col2")
	c.Check(getResp.Columns[0].Value, Equals, int64(77))
	c.Check(getResp.Columns[1].ColumnName, Equals, "col3")
	c.Check(getResp.Columns[1].Value, Equals, "newcol3")

	deleteRowReq := new(DeleteRowRequest)
	deleteRowReq.DeleteRowChange = new(DeleteRowChange)
	deleteRowReq.DeleteRowChange.TableName = defaultTableName
	deletePk := new(PrimaryKey)
	deletePk.AddPrimaryKeyColumn("pk1", keyToUpdate)
	deleteRowReq.DeleteRowChange.PrimaryKey = deletePk
	deleteRowReq.DeleteRowChange.SetCondition(RowExistenceExpectation_EXPECT_EXIST)
	clCondition1 := NewSingleColumnCondition("col2", CT_EQUAL, int64(77))
	deleteRowReq.DeleteRowChange.SetColumnCondition(clCondition1)
	resp, error := client.DeleteRow(deleteRowReq)
	c.Check(error, Equals, nil)
	fmt.Println(resp.ConsumedCapacityUnit.Write)
	fmt.Println(resp.ConsumedCapacityUnit.Read)

	_, error = invalidClient.UpdateRow(updateRowRequest)
	c.Check(error, NotNil)

	_, error = invalidClient.DeleteRow(deleteRowReq)
	c.Check(error, NotNil)

	fmt.Println("TestPutUpdateDeleteRow finished")
}

func (s *TableStoreSuite) TestBatchGetRow(c *C) {
	fmt.Println("TestBatchGetRow started")
	rowCount := 100
	for i := 0; i < rowCount; i++ {
		key := "batchkey" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		PrepareDataInDefaultTable(key, value)
	}

	batchGetReq := &BatchGetRowRequest{}
	mqCriteria := &MultiRowQueryCriteria{}

	for i := 0; i < rowCount; i++ {
		pkToGet := new(PrimaryKey)
		key := "batchkey" + strconv.Itoa(i)
		pkToGet.AddPrimaryKeyColumn("pk1", key)
		mqCriteria.AddRow(pkToGet)
	}
	mqCriteria.MaxVersion = 1
	mqCriteria.TableName = defaultTableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, error := client.BatchGetRow(batchGetReq)
	c.Check(error, Equals, nil)

	c.Check(len(batchGetResponse.TableToRowsResult), Equals, 1)
	c.Check(len(batchGetResponse.TableToRowsResult[mqCriteria.TableName]), Equals, rowCount)

	for index, rowToCheck := range (batchGetResponse.TableToRowsResult[mqCriteria.TableName]) {
		c.Check(rowToCheck.Index, Equals, int32(index))
		c.Check(rowToCheck.TableName, Equals, mqCriteria.TableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
		c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 1)
		c.Check(len(rowToCheck.Columns), Equals, 1)
	}

	batchGetReq = &BatchGetRowRequest{}
	mqCriteria = &MultiRowQueryCriteria{}

	for i := 0; i < rowCount; i++ {
		pkToGet := new(PrimaryKey)
		key := "batchkey" + strconv.Itoa(i)
		pkToGet.AddPrimaryKeyColumn("pk1", key)
		mqCriteria.AddRow(pkToGet)
		mqCriteria.AddColumnToGet("col1")
	}
	timeNow := time.Now().Unix() * 1000
	mqCriteria.TimeRange = &TimeRange{Start: timeNow - 10000, End : timeNow + 10000}
	mqCriteria.TableName = defaultTableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, error = client.BatchGetRow(batchGetReq)
	c.Check(error, Equals, nil)
	c.Check(len(batchGetResponse.TableToRowsResult), Equals, 1)
	c.Check(len(batchGetResponse.TableToRowsResult[mqCriteria.TableName]), Equals, rowCount)

	for index, rowToCheck := range (batchGetResponse.TableToRowsResult[mqCriteria.TableName]) {
		c.Check(rowToCheck.TableName, Equals, mqCriteria.TableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
		c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 1)
		c.Check(len(rowToCheck.Columns), Equals, 1)
		c.Check(rowToCheck.Index, Equals, int32(index))
	}

	// test timerange
	batchGetReq = &BatchGetRowRequest{}
	mqCriteria = &MultiRowQueryCriteria{}

	for i := 0; i < rowCount; i++ {
		pkToGet := new(PrimaryKey)
		key := "batchkey" + strconv.Itoa(i)
		pkToGet.AddPrimaryKeyColumn("pk1", key)
		mqCriteria.AddRow(pkToGet)
	}
	mqCriteria.TimeRange = &TimeRange{Start: timeNow + 10000, End : timeNow + 20000}
	mqCriteria.TableName = defaultTableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, error = client.BatchGetRow(batchGetReq)
	c.Check(error, Equals, nil)

	c.Check(len(batchGetResponse.TableToRowsResult), Equals, 1)
	c.Check(len(batchGetResponse.TableToRowsResult[mqCriteria.TableName]), Equals, rowCount)

	for index, rowToCheck := range (batchGetResponse.TableToRowsResult[mqCriteria.TableName]) {
		c.Check(rowToCheck.TableName, Equals, mqCriteria.TableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
		c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 1)
		c.Check(len(rowToCheck.Columns), Equals, 0)
		c.Check(rowToCheck.Index, Equals, int32(index))
	}
	_, error = invalidClient.BatchGetRow(batchGetReq)
	c.Check(error, NotNil)

	fmt.Println("TestBatchGetRow started")
}

func (s *TableStoreSuite) TestBatchGetRowWithFilter(c *C) {
	fmt.Println("TestBatchGetRowWithFilter started")
	rowCount := 100
	for i := 0; i < rowCount; i++ {
		key := "filterbatchkey" + strconv.Itoa(i)
		value1 := "col0value" + strconv.Itoa(i)
		value2 := "col1value" + strconv.Itoa(i)
		value3 := "col2value" + strconv.Itoa(i)
		PrepareDataInDefaultTableWithMultiAttribute(key, value1, value2, value3)
	}

	// pagination filter
	pagedFilter := &PaginationFilter{}
	pagedFilter.Limit = 2
	pagedFilter.Offset = 1

	batchGetReq := &BatchGetRowRequest{}
	mqCriteria := &MultiRowQueryCriteria{}
	mqCriteria.SetFilter(pagedFilter)

	for i := 0; i < rowCount; i++ {
		pkToGet := new(PrimaryKey)
		key := "filterbatchkey" + strconv.Itoa(i)
		pkToGet.AddPrimaryKeyColumn("pk1", key)
		mqCriteria.AddRow(pkToGet)
	}

	mqCriteria.MaxVersion = 1
	mqCriteria.TableName = defaultTableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, error := client.BatchGetRow(batchGetReq)
	c.Check(error, Equals, nil)

	c.Check(len(batchGetResponse.TableToRowsResult), Equals, 1)
	c.Check(len(batchGetResponse.TableToRowsResult[mqCriteria.TableName]), Equals, rowCount)

	for index, rowToCheck := range (batchGetResponse.TableToRowsResult[mqCriteria.TableName]) {
		c.Check(rowToCheck.TableName, Equals, mqCriteria.TableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
		c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 1)
		c.Check(len(rowToCheck.Columns), Equals, 2)
		c.Check(rowToCheck.Index, Equals, int32(index))
	}

	// compsite filter
	batchGetReq = &BatchGetRowRequest{}
	clCondition1 := NewSingleColumnCondition("col1", CT_EQUAL, "col0value1")
	clCondition2 := NewSingleColumnCondition("col2", CT_EQUAL, "col1value1")

	cf := NewCompositeColumnCondition(LO_AND)
	cf.AddFilter(clCondition1)
	cf.AddFilter(clCondition2)

	mqCriteria = &MultiRowQueryCriteria{}
	mqCriteria.SetFilter(cf)

	for i := 0; i < rowCount; i++ {
		pkToGet := new(PrimaryKey)
		key := "filterbatchkey" + strconv.Itoa(i)
		pkToGet.AddPrimaryKeyColumn("pk1", key)
		mqCriteria.AddRow(pkToGet)
	}

	mqCriteria.MaxVersion = 1
	mqCriteria.TableName = defaultTableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, error = client.BatchGetRow(batchGetReq)
	c.Check(error, Equals, nil)

	c.Check(len(batchGetResponse.TableToRowsResult), Equals, 1)
	c.Check(len(batchGetResponse.TableToRowsResult[mqCriteria.TableName]), Equals, rowCount)

	count :=0
	for index, rowToCheck := range (batchGetResponse.TableToRowsResult[mqCriteria.TableName]) {
		c.Check(rowToCheck.Index, Equals, int32(index))
		c.Check(rowToCheck.TableName, Equals, mqCriteria.TableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)

		if len(rowToCheck.PrimaryKey.PrimaryKeys) > 0 {
			c.Check(len(rowToCheck.Columns), Equals, 3)
			count++
		}
	}
	c.Check(count, Equals, 1)

	fmt.Println("TestBatchGetRowWithFilter finished")
}

func (s *TableStoreSuite) TestBatchWriteRow(c *C) {
	fmt.Println("TestBatchWriteRow started")

	PrepareDataInDefaultTable("updateinbatchkey1", "updateinput1")
	PrepareDataInDefaultTable("deleteinbatchkey1", "deleteinput1")
	batchWriteReq := &BatchWriteRowRequest{}

	rowToPut1 := CreatePutRowChange("putinbatchkey1", "datainput1")
	rowToPut2 := CreatePutRowChange("putinbatchkey2", "datainput2")

	updateRowChange := new(UpdateRowChange)
	updateRowChange.TableName = defaultTableName
	updatePk := new(PrimaryKey)
	updatePk.AddPrimaryKeyColumn("pk1", "updateinbatchkey1")
	updateRowChange.PrimaryKey = updatePk
	updateRowChange.DeleteColumn("col1")
	updateRowChange.PutColumn("col2", int64(77))
	updateRowChange.PutColumn("col3", "newcol3")
	updateRowChange.SetCondition(RowExistenceExpectation_EXPECT_EXIST)

	deleteRowChange := new(DeleteRowChange)
	deleteRowChange.TableName = defaultTableName
	deletePk := new(PrimaryKey)
	deletePk.AddPrimaryKeyColumn("pk1", "deleteinbatchkey1")
	deleteRowChange.PrimaryKey = deletePk
	deleteRowChange.SetCondition(RowExistenceExpectation_EXPECT_EXIST)

	batchWriteReq.AddRowChange(rowToPut1)
	batchWriteReq.AddRowChange(rowToPut2)
	batchWriteReq.AddRowChange(updateRowChange)
	batchWriteReq.AddRowChange(deleteRowChange)

	batchWriteResponse, error := client.BatchWriteRow(batchWriteReq)
	c.Check(error, Equals, nil)
	c.Check(len(batchWriteResponse.TableToRowsResult), Equals, 1)

	for index, rowToCheck := range (batchWriteResponse.TableToRowsResult[defaultTableName]) {
		c.Check(rowToCheck.Index, Equals, int32(index))
		c.Check(rowToCheck.TableName, Equals, defaultTableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
	}

	_, error = invalidClient.BatchWriteRow(batchWriteReq)
	c.Check(error, NotNil)

	fmt.Println("TestBatchWriteRow finished")
}

func (s *TableStoreSuite) TestGetRange(c *C) {
	fmt.Println("TestGetRange started")
	rowCount := 9
	timeNow := time.Now().Unix() * 1000
	for i := 0; i < rowCount; i++ {
		key := "getrange" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		PrepareDataInDefaultTableWithTimestamp(key, value, timeNow)
	}

	getRangeRequest := &GetRangeRequest{}
	rangeRowQueryCriteria := &RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = defaultTableName
	start := 1
	end := 8
	startPK := new(PrimaryKey)
	startPK.AddPrimaryKeyColumn("pk1", "getrange" + strconv.Itoa(start))
	endPK := new(PrimaryKey)
	endPK.AddPrimaryKeyColumn("pk1", "getrange" + strconv.Itoa(end))
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	rangeRowQueryCriteria.EndPrimaryKey = endPK
	rangeRowQueryCriteria.Direction = FORWARD
	rangeRowQueryCriteria.MaxVersion = 1
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria

	getRangeResp, error := client.GetRange(getRangeRequest)
	c.Check(error, Equals, nil)
	c.Check(getRangeResp.Rows, NotNil)
	count := end - start
	c.Check(len(getRangeResp.Rows), Equals, count)
	c.Check(len(getRangeResp.Rows[0].Columns), Equals, 1)
	c.Check(getRangeResp.NextStartPrimaryKey, IsNil)

	getRangeRequest = &GetRangeRequest{}
	rangeRowQueryCriteria = &RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = defaultTableName

	rangeRowQueryCriteria.StartPrimaryKey = endPK
	rangeRowQueryCriteria.EndPrimaryKey = startPK
	rangeRowQueryCriteria.Direction = BACKWARD
	rangeRowQueryCriteria.MaxVersion = 1
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria
	getRangeResp, error = client.GetRange(getRangeRequest)
	c.Check(error, Equals, nil)
	c.Check(getRangeResp.Rows, NotNil)

	fmt.Println("use time range to query rows")

	rangeRowQueryCriteria.TimeRange = &TimeRange{Specific: timeNow - 100001}
	getRangeResp, error = client.GetRange(getRangeRequest)
	c.Check(error, NotNil)
	fmt.Println(error)

	fmt.Println("use time range to query rows 2")
	rangeRowQueryCriteria.TimeRange = &TimeRange{Start: timeNow + 1, End: timeNow + 2}
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria
	getRangeResp2, error := client.GetRange(getRangeRequest)

	c.Check(error, Equals, nil)
	c.Check(getRangeResp2.Rows, NotNil)
	c.Check(len(getRangeResp2.Rows), Equals, count)
	c.Check(len(getRangeResp2.Rows[0].Columns), Equals, 0)

	_, error = invalidClient.GetRange(getRangeRequest)
	c.Check(error, NotNil)
	fmt.Println("TestGetRange finished")
}

func (s *TableStoreSuite) TestGetRangeWithPagination(c *C) {
	fmt.Println("TestGetRangeWithPagination started")
	rowCount := 9
	for i := 0; i < rowCount; i++ {
		key := "testrangequery" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		PrepareDataInDefaultTable(key, value)
	}

	getRangeRequest := &GetRangeRequest{}
	rangeRowQueryCriteria := &RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = defaultTableName
	start := 1
	end := 8
	var limit int32 = 3
	startPK := new(PrimaryKey)
	startPK.AddPrimaryKeyColumn("pk1", "testrangequery" + strconv.Itoa(start))
	endPK := new(PrimaryKey)
	endPK.AddPrimaryKeyColumn("pk1", "testrangequery" + strconv.Itoa(end))
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	rangeRowQueryCriteria.EndPrimaryKey = endPK
	rangeRowQueryCriteria.Direction = FORWARD
	rangeRowQueryCriteria.MaxVersion = 1
	rangeRowQueryCriteria.Limit = limit
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria

	getRangeResp, error := client.GetRange(getRangeRequest)

	c.Check(error, Equals, nil)
	c.Check(getRangeResp.Rows, NotNil)

	c.Check(len(getRangeResp.Rows), Equals, int(limit))
	c.Check(getRangeResp.NextStartPrimaryKey, NotNil)
	fmt.Println("TestGetRangeWithPagination finished")
}

func (s *TableStoreSuite) TestGetRangeWithFilter(c *C) {
	fmt.Println("TestGetRange started")
	rowCount := 20
	timeNow := time.Now().Unix() * 1000
	for i := 0; i < rowCount; i++ {
		key := "zgetrangetest" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		PrepareDataInRangeTableWithTimestamp("pk1",key, value, timeNow)
	}

	for i := 0; i < rowCount; i++ {
		key := "zgetrangetest2" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		PrepareDataInRangeTableWithTimestamp("pk2", key, value, timeNow)
	}

	for i := 0; i < rowCount; i++ {
		key := "zgetrangetest3" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		PrepareDataInRangeTableWithTimestamp("pk3", key, value, timeNow)
	}

	getRangeRequest := &GetRangeRequest{}
	rangeRowQueryCriteria := &RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = rangeQueryTableName

	startPK := new(PrimaryKey)
	startPK.AddPrimaryKeyColumnWithMinValue("pk1")
	startPK.AddPrimaryKeyColumnWithMinValue("pk2")
	endPK := new(PrimaryKey)
	endPK.AddPrimaryKeyColumnWithMaxValue("pk1")
	endPK.AddPrimaryKeyColumnWithMaxValue("pk2")
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	rangeRowQueryCriteria.EndPrimaryKey = endPK
	rangeRowQueryCriteria.Direction = FORWARD
	rangeRowQueryCriteria.MaxVersion = 1
	filter := NewCompositeColumnCondition(LogicalOperator(LO_AND))
	filter1:= NewSingleColumnCondition("pk2", ComparatorType(CT_GREATER_EQUAL), "pk3")
	filter2:= NewSingleColumnCondition("pk2", ComparatorType(CT_LESS_EQUAL), "pk3")
	filter.AddFilter(filter2)
	filter.AddFilter(filter1)
	rangeRowQueryCriteria.Filter = filter
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria

	getRangeResp, error := client.GetRange(getRangeRequest)
	c.Check(error, Equals, nil)
	fmt.Println(getRangeResp)
	fmt.Println(getRangeResp.NextStartPrimaryKey)
	fmt.Println(getRangeResp.Rows)
	//fmt.Println(getRangeResp.NextStartPrimaryKey)
	//c.Check(getRangeResp.Rows, NotNil)

	fmt.Println("TestGetRange with filter finished")
}

func (s *TableStoreSuite) TestGetRangeWithMinMaxValue(c *C) {
	fmt.Println("TestGetRangeWithMinMaxValue started")

	getRangeRequest := &GetRangeRequest{}
	rangeRowQueryCriteria := &RangeRowQueryCriteria{}
	rangeRowQueryCriteria.TableName = defaultTableName

	var limit int32 = 8
	startPK := new(PrimaryKey)
	startPK.AddPrimaryKeyColumnWithMinValue("pk1")
	endPK := new(PrimaryKey)
	endPK.AddPrimaryKeyColumnWithMaxValue("pk1")
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	rangeRowQueryCriteria.EndPrimaryKey = endPK
	rangeRowQueryCriteria.Direction = FORWARD
	rangeRowQueryCriteria.MaxVersion = 1
	rangeRowQueryCriteria.Limit = limit
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria

	getRangeResp, error := client.GetRange(getRangeRequest)

	c.Check(error, Equals, nil)
	c.Check(getRangeResp.Rows, NotNil)

	c.Check(len(getRangeResp.Rows), Equals, int(limit))
	c.Check(getRangeResp.NextStartPrimaryKey, NotNil)
	fmt.Println("TestGetRangeWithMinMaxValue finished")
}

func (s *TableStoreSuite) TestPutRowsWorkload(c *C) {
	fmt.Println("TestPutRowsWorkload started")

	start:= time.Now().UnixNano()

	isFinished := make(chan bool)
	totalCount := 100
	for i := 0; i < totalCount; i++ {
		value := i * 10000
		go func(index int) {
			for j := 0; j < 100; j++ {
				currentIndex := index + j
				rowToPut1 := CreatePutRowChange("workloadtestkey" + strconv.Itoa(currentIndex), "perfdata1")
				putRowRequest := new(PutRowRequest)
				putRowRequest.PutRowChange = rowToPut1
				_, error := client.PutRow(putRowRequest)
				if error !=nil {
					fmt.Println("put row error", error)
				}
				c.Check(error, IsNil)
			}

			isFinished <- true
		}(value)
	}

	/*go func(){
		time.Sleep(time.Millisecond * 1000 * 10)
		close(isFinished)
	}()*/

	count := 0
	for _ = range isFinished {
		count++
		fmt.Println("catched count is:", count)
		if count >=totalCount {
			close(isFinished)
		}
	}
	c.Check(count, Equals, totalCount)
	end := time.Now().UnixNano()

	totalCost := (end - start) / 1000000
	fmt.Println("total cost:", totalCost)
	c.Check(totalCost < 30 * 1000, Equals, true)

	time.Sleep(time.Millisecond * 20)
	fmt.Println("TestPutRowsWorkload finished")
}

func (s *TableStoreSuite) TestFailureCase(c *C) {
	tableName := randStringRunes(200)
	createtableRequest := new(CreateTableRequest)
	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput
	_, error := client.CreateTable(createtableRequest)
	c.Check(error, NotNil)
	c.Check(error.Error(), Equals, errTableNameTooLong(tableName).Error())

	createtableRequest = new(CreateTableRequest)
	tableMeta = new(TableMeta)
	tableMeta.TableName = tableNamePrefix + "pktomuch"

	tableOption = new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput = new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput
	_, error = client.CreateTable(createtableRequest)
	c.Check(error, NotNil)
	c.Check(error.Error(), Equals, errCreateTableNoPrimaryKey.Error())

	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk2", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk3", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk4", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk5", PrimaryKeyType_STRING)

	_, error = client.CreateTable(createtableRequest)
	c.Check(error, NotNil)
	c.Check(error.Error(), Equals, errPrimaryKeyTooMuch.Error())

	request := &PutRowRequest{}
	_, error = client.PutRow(request)
	c.Check(error, IsNil)

	_, error = client.PutRow(nil)
	c.Check(error, IsNil)

	_, err := invalidClient.ListTable()
	c.Check(err, NotNil)

	tableName = tableNamePrefix + "tablenotexist"
	deleteReq := new(DeleteTableRequest)
	deleteReq.TableName = tableName
	_,err = client.DeleteTable(deleteReq)
	c.Check(err, NotNil)

	_, err = invalidClient.ListTable()
	c.Check(err, NotNil)

	updateTableReq := new(UpdateTableRequest)
	updateTableReq.TableName = defaultTableName
	updateTableReq.TableOption = new(TableOption)
	updateTableReq.TableOption.TimeToAlive = -1
	updateTableReq.TableOption.MaxVersion = 5
	updateTableReq.ReservedThroughput = &ReservedThroughput{}
	updateTableReq.ReservedThroughput.Readcap = 0

	_, error = invalidClient.UpdateTable(updateTableReq)
	c.Assert(error, NotNil)


	describeTableReq := new(DescribeTableRequest)
	describeTableReq.TableName = defaultTableName
	_, error = invalidClient.DescribeTable(describeTableReq)
	c.Assert(error, NotNil)
}

func (s *TableStoreSuite) TestMockHttpClientCase(c *C) {
	fmt.Println("TestMockHttpClientCase started")
	currentGetHttpClientFunc = func() IHttpClient {
		return &mockHttpClient{}
	}

	tempClient := NewClientWithConfig("test","a","b","c","d", NewDefaultTableStoreConfig())
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", "mockkey1")
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", "col1data1")
	putRowChange.AddColumn("col2", int64(100))
	putRowChange.AddColumn("col3", float64(2.1))
	putRowChange.SetCondition(RowExistenceExpectation_EXPECT_NOT_EXIST)
	putRowRequest.PutRowChange = putRowChange
	data :=tempClient.httpClient.(*mockHttpClient)

	data.error = fmt.Errorf("test")
	_, error := tempClient.PutRow(putRowRequest)
	c.Check(error, Equals, data.error)

	data.response = &http.Response{}
	_, error = tempClient.PutRow(putRowRequest)
	c.Check(error, Equals, data.error)

	/*data.error = nil
	_, error = tempClient.PutRow(putRowRequest)
	c.Check(error, Equals, data.error)*/

	currentGetHttpClientFunc = func() IHttpClient {
		return &TableStoreHttpClient{}
	}

	fmt.Println("TestMockHttpClientCase finished")
}

func (s *TableStoreSuite) TestUnit(c *C) {
	otshead := createOtsHeaders("test")
	otshead.set(xOtsApiversion, ApiVersion)
	_, error := otshead.signature(getRowUri, "POST", "test")
	c.Check(error, NotNil)

	otshead.set(xOtsDate, "any")
	otshead.set(xOtsApiversion, "any")
	otshead.set(xOtsAccesskeyid, "any")
	otshead.set(xOtsContentmd5, "any")
	otshead.set(xOtsInstanceName, "any")

	otshead.headers = nil
	otshead.set("abc", "def")

	result := otshead.search("zz")
	c.Check(result, IsNil)

	tempClient := NewClient("a","b","c", "d", SetSth())
	c.Check(tempClient, NotNil)
	config := NewDefaultTableStoreConfig()
	tempClient = NewClientWithConfig("a","b","c", "d", "e", config)
	c.Check(tempClient, NotNil)

	errorCode := INTERNAL_SERVER_ERROR
	tsClient := client.(*TableStoreClient)
	value := getNextPause(tsClient, nil, &tsprotocol.Error{Code: &errorCode, Message: &errorCode}, 10, time.Now().Add(time.Second * 1), 10, getRowUri, 500)
	c.Check(value == 0, Equals, true)

	errorCode = ROW_OPERATION_CONFLICT
	value = getNextPause(tsClient, nil, &tsprotocol.Error{Code: &errorCode, Message: &errorCode}, 1, time.Now().Add(time.Second * 1), 10, getRowUri, 500)
	c.Check(value > 0, Equals, true)

	errorCode = STORAGE_TIMEOUT
	value = getNextPause(tsClient, nil, &tsprotocol.Error{Code: &errorCode, Message: &errorCode}, 1, time.Now().Add(time.Second * 1), 10, putRowUri, 500)
	c.Check(value == 0, Equals, true)

	errorCode = STORAGE_TIMEOUT
	value = getNextPause(tsClient, nil, &tsprotocol.Error{Code: &errorCode, Message: &errorCode}, 1, time.Now().Add(time.Second * 1), 10, getRowUri, 500)
	c.Check(value > 0, Equals, true)

	errorCode = STORAGE_TIMEOUT
	value = getNextPause(tsClient, nil, &tsprotocol.Error{Code: &errorCode, Message: &errorCode}, 1, time.Now().Add(time.Second * 1), MaxRetryInterval, getRowUri, 500)
	c.Check(value == MaxRetryInterval, Equals, true)

	getResp := &GetRowResponse{}
	colMap := getResp.GetColumnMap()
	c.Check(colMap, NotNil)

	getResp = &GetRowResponse{}
	col1 := &AttributeColumn{ColumnName:"col1", Value:"value1"}
	col2 := &AttributeColumn{ColumnName:"col1", Value:"value2"}
	col3 := &AttributeColumn{ColumnName:"col2", Value:"value3"}

	getResp.Columns = append(getResp.Columns, col1)
	getResp.Columns = append(getResp.Columns, col2)
	getResp.Columns = append(getResp.Columns, col3)
	colMap = getResp.GetColumnMap()
	c.Check(colMap, NotNil)
	cols := colMap.Columns["col1"]
	c.Check(cols, NotNil)
	c.Check(len(cols), Equals, 2)

	cols2 := colMap.Columns["col2"]
	c.Check(cols2, NotNil)
	c.Check(len(cols2), Equals, 1)

	cols3, _ := colMap.GetRange(1, 1)

	c.Check(cols3, NotNil)
	c.Check(len(cols3), Equals, 1)

	var resp2 *GetRowResponse
	resp2 = nil
	c.Check(resp2.GetColumnMap(), IsNil)
}

func SetSth() ClientOption{
	return func(client *TableStoreClient) {
		fmt.Println(client.accessKeyId)
	}
}

func CreatePutRowChange(pkValue, colValue string) *PutRowChange {
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", pkValue)
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", colValue)
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	return putRowChange
}

type mockHttpClient struct {
	response *http.Response
	error error
	httpClient      *http.Client
}

func (mockHttpClient *mockHttpClient) Do(req *http.Request) (*http.Response, error) {
	return mockHttpClient.response, mockHttpClient.error
}

func (mockHttpClient *mockHttpClient) New(client *http.Client) {
	mockHttpClient.httpClient = client
}


var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
func randStringRunes(n int) string {
	random := rand.New(rand.NewSource(time.Now().Unix()))

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[random.Intn(len(letterRunes))]
	}
	return string(b)
}

func PrepareDataInDefaultTable(key string, value string) error {
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", key)
	putRowChange.AddColumn("col1", value)
	putRowChange.PrimaryKey = putPk
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	return error
}

func PrepareDataInDefaultTableWithMultiAttribute(key string, value1 string, value2 string, value3 string) error {
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", key)
	putRowChange.AddColumn("col1", value1)
	putRowChange.AddColumn("col2", value2)
	putRowChange.AddColumn("col3", value3)
	putRowChange.PrimaryKey = putPk
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	return error
}

func PrepareDataInDefaultTableWithTimestamp(key string, value string, timeNow int64) error {
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", key)
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumnWithTimestamp("col1", value, timeNow)
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	return error
}

func PrepareDataInRangeTableWithTimestamp(key1 string, key2 string, value string, timeNow int64) error {
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = rangeQueryTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", key1)
	putPk.AddPrimaryKeyColumn("pk2", key2)
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumnWithTimestamp("col1", value, timeNow)
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	return error
}
