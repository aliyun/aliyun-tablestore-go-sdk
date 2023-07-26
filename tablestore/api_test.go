package tablestore

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	. "gopkg.in/check.v1"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type TableStoreSuite struct{}

var tableNamePrefix string

var _ = Suite(&TableStoreSuite{})

var (
	defaultTableName       = "defaulttable"
	rangeQueryTableName    = "rangetable"
	sqlTableName           = "test_http_query"
	sqlTableNameWithSearch = "test_sql_with_search"
	sqlSearchName          = "test_sql_with_search_index"

	fuzzyTableName = "fuzzytable"
	fuzzyMetaPk1   = "pkStr"
	fuzzyMetaPk2   = "pkBlob"
	fuzzyMetaPk3   = "pkInt"
	fuzzyMetaAttr  = []string{"string", "integer", "boolean", "double", "blob"}
)

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
	rangeQueryTableName = tableNamePrefix + rangeQueryTableName
	PrepareTable(defaultTableName)
	PrepareTable2(rangeQueryTableName)
	err := PrepareFuzzyTable(fuzzyTableName)
	c.Assert(err, IsNil)
	// prepare sql tables
	deleteTable(sqlTableName)
	deleteSearchIndex(sqlTableNameWithSearch, sqlSearchName)
	deleteTable(sqlTableNameWithSearch)

	PrepareSQLTable(sqlTableName)
	PrepareSQLTable(sqlTableNameWithSearch)
	PrepareSQLSearchIndex(c, sqlTableNameWithSearch, sqlSearchName)
	WaitDataSyncByMatchAllQuery(c, client, 4, sqlTableNameWithSearch, sqlSearchName, 40)

	invalidClient = NewClient(endpoint, instanceName, accessKeyId, "invalidsecret")
}

func (s *TableStoreSuite) SetUpTest(c *C) {

}

func (s *TableStoreSuite) TearDownTest(c *C) {

}

func PrepareFuzzyTable(tableName string) error {
	client.DeleteTable(&DeleteTableRequest{TableName: tableName})
	time.Sleep(time.Second)
	meta := &TableMeta{
		TableName: tableName,
	}
	meta.AddPrimaryKeyColumn(fuzzyMetaPk1, PrimaryKeyType_STRING)
	meta.AddPrimaryKeyColumn(fuzzyMetaPk2, PrimaryKeyType_BINARY)
	meta.AddPrimaryKeyColumn(fuzzyMetaPk3, PrimaryKeyType_INTEGER)
	req := &CreateTableRequest{
		TableMeta: meta,
		TableOption: &TableOption{
			TimeToAlive: -1,
			MaxVersion:  1,
		},
		ReservedThroughput: &ReservedThroughput{0, 0},
	}
	_, err := client.CreateTable(req)
	time.Sleep(time.Second)
	return err
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

func PrepareSQLTable(tableName string) {
	createtableRequest := new(CreateTableRequest)
	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("a", PrimaryKeyType_INTEGER)
	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput
	_, _ = client.CreateTable(createtableRequest)

	time.Sleep(2 * time.Second)
	batchReq := new(BatchWriteRowRequest)
	rowChange := new(PutRowChange)
	rowChange.TableName = tableName
	rowChange.PrimaryKey = new(PrimaryKey)
	rowChange.PrimaryKey.AddPrimaryKeyColumn("a", int64(0))
	rowChange.AddColumn("b", 0.0)
	rowChange.AddColumn("c", "0")
	rowChange.AddColumn("d", []byte("0"))
	rowChange.AddColumn("e", false)
	rowChange.SetCondition(RowExistenceExpectation_IGNORE)
	batchReq.AddRowChange(rowChange)

	rowChange = new(PutRowChange)
	rowChange.TableName = tableName
	rowChange.PrimaryKey = new(PrimaryKey)
	rowChange.PrimaryKey.AddPrimaryKeyColumn("a", int64(1))
	rowChange.AddColumn("b", 1.0)
	rowChange.AddColumn("c", "1")
	rowChange.AddColumn("d", []byte("1"))
	rowChange.AddColumn("e", true)
	rowChange.SetCondition(RowExistenceExpectation_IGNORE)
	batchReq.AddRowChange(rowChange)

	rowChange = new(PutRowChange)
	rowChange.TableName = tableName
	rowChange.PrimaryKey = new(PrimaryKey)
	rowChange.PrimaryKey.AddPrimaryKeyColumn("a", int64(2))
	rowChange.AddColumn("b", 2.0)
	rowChange.AddColumn("c", "2")
	rowChange.AddColumn("d", []byte("2"))
	rowChange.AddColumn("e", false)
	rowChange.SetCondition(RowExistenceExpectation_IGNORE)
	batchReq.AddRowChange(rowChange)

	rowChange = new(PutRowChange)
	rowChange.TableName = tableName
	rowChange.PrimaryKey = new(PrimaryKey)
	rowChange.PrimaryKey.AddPrimaryKeyColumn("a", int64(3))
	rowChange.AddColumn("b", 3.0)
	rowChange.AddColumn("e", true)
	rowChange.SetCondition(RowExistenceExpectation_IGNORE)
	batchReq.AddRowChange(rowChange)

	_, err := client.BatchWriteRow(batchReq)
	if err != nil {
		log.Println("batchwriterow failed", err.Error())
	}
}

func PrepareSQLSearchIndex(c *C, tableName string, indexName string) {
	log.Println("Begin to create index:", searchAPITestIndexName1)
	request := &CreateSearchIndexRequest{}
	request.TableName = tableName
	request.IndexName = indexName

	var schemas []*FieldSchema
	field1 := &FieldSchema{
		FieldName:        proto.String("a"),
		FieldType:        FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field2 := &FieldSchema{
		FieldName:        proto.String("b"),
		FieldType:        FieldType_DOUBLE,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field3 := &FieldSchema{
		FieldName:        proto.String("c"),
		FieldType:        FieldType_KEYWORD,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field4 := &FieldSchema{
		FieldName:        proto.String("e"),
		FieldType:        FieldType_BOOLEAN,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	schemas = append(schemas, field1, field2, field3, field4)

	request.IndexSchema = &IndexSchema{
		FieldSchemas: schemas,
	}
	_, err := client.CreateSearchIndex(request)
	if err != nil {
		log.Println("failed to create search index with error: ", err.Error())
		c.Fatal("Failed to create search index with error: ", err)
	} else {
		log.Println("Create search index finished")
	}
}

func (s *TableStoreSuite) TestCreateTable(c *C) {
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
	log.Println("TestCreateTable finished")
}

func (s *TableStoreSuite) TestCreateTableWithOriginColumn(c *C) {
	tableName := tableNamePrefix + "originColumn"
	defer func() {
		deleteReq := new(DeleteTableRequest)
		deleteReq.TableName = tableName
		client.DeleteTable(deleteReq)
	}()

	ctReq := new(CreateTableRequest)
	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)

	tableOption := new(TableOption)

	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3

	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0

	ctReq.TableMeta = tableMeta
	ctReq.TableOption = tableOption
	ctReq.ReservedThroughput = reservedThroughput

	ctReq.StreamSpec = &StreamSpecification{
		EnableStream:       true,
		ExpirationTime:     168,
		OriginColumnsToGet: []string{"col1", "col2"},
	}

	_, err := client.CreateTable(ctReq)
	c.Check(err, Equals, nil)

	descTableRequest := &DescribeTableRequest{
		TableName: tableName,
	}
	descResp, err := client.DescribeTable(descTableRequest)
	c.Check(err, Equals, nil)
	c.Check(2, Equals, len(descResp.StreamDetails.OriginColumnsToGet))

	log.Println("TestCreateTableWithOriginColumn finished")
}

func (s *TableStoreSuite) TestReCreateTableAndPutRow(c *C) {
	log.Println("TestReCreateTableAndPutRow started")

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

	log.Println("TestReCreateTableAndPutRow finished")
}

func (s *TableStoreSuite) TestListTable(c *C) {
	listtables, error := client.ListTable()
	c.Check(error, Equals, nil)
	defaultTableExist := false
	for _, table := range listtables.TableNames {
		log.Println(table)
		if table == defaultTableName {
			defaultTableExist = true
			break
		}
	}

	c.Check(defaultTableExist, Equals, true)
}

func (s *TableStoreSuite) TestUpdateAndDescribeTable(c *C) {
	log.Println("TestUpdateAndDescribeTable started")
	updateTableReq := new(UpdateTableRequest)
	updateTableReq.TableName = defaultTableName
	updateTableReq.TableOption = new(TableOption)
	updateTableReq.TableOption.TimeToAlive = -1
	updateTableReq.TableOption.MaxVersion = 5
	updateTableReq.StreamSpec = new(StreamSpecification)
	updateTableReq.StreamSpec.EnableStream = true
	updateTableReq.StreamSpec.ExpirationTime = 168
	updateTableReq.StreamSpec.OriginColumnsToGet = []string{"col1", "col2"}

	updateTableResp, error := client.UpdateTable(updateTableReq)
	c.Assert(error, Equals, nil)
	c.Assert(updateTableResp, NotNil)
	c.Assert(updateTableResp.TableOption.TimeToAlive, Equals, updateTableReq.TableOption.TimeToAlive)
	c.Assert(updateTableResp.TableOption.MaxVersion, Equals, updateTableReq.TableOption.MaxVersion)
	c.Assert(updateTableResp.StreamDetails.EnableStream, Equals, updateTableReq.StreamSpec.EnableStream)
	c.Assert(updateTableResp.StreamDetails.ExpirationTime, Equals, updateTableReq.StreamSpec.ExpirationTime)

	describeTableReq := new(DescribeTableRequest)
	describeTableReq.TableName = defaultTableName
	describ, error := client.DescribeTable(describeTableReq)
	c.Assert(error, Equals, nil)

	c.Assert(describ, NotNil)
	c.Assert(describ.TableOption.TimeToAlive, Equals, updateTableReq.TableOption.TimeToAlive)
	c.Assert(describ.TableOption.MaxVersion, Equals, updateTableReq.TableOption.MaxVersion)
	c.Assert(describ.StreamDetails.EnableStream, Equals, updateTableReq.StreamSpec.EnableStream)
	c.Assert(describ.StreamDetails.ExpirationTime, Equals, updateTableReq.StreamSpec.ExpirationTime)
	c.Assert(len(describ.StreamDetails.OriginColumnsToGet), Equals, len(updateTableReq.StreamSpec.OriginColumnsToGet))
	for i, s := range describ.StreamDetails.OriginColumnsToGet {
		c.Assert(s, Equals, updateTableReq.StreamSpec.OriginColumnsToGet[i])
	}
	log.Println("TestUpdateAndDescribeTable finished")
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
		putPk.AddPrimaryKeyColumn("pk1", "key"+strconv.Itoa(i))
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
		c.Check(response.PrimaryKey.PrimaryKeys[0].Value, Equals, "key"+strconv.Itoa(i))
		c.Check(response.PrimaryKey.PrimaryKeys[1].ColumnName, Equals, "pk2")
		c.Check(response.PrimaryKey.PrimaryKeys[1].Value.(int64) > 0, Equals, true)

		log.Println(response.PrimaryKey.PrimaryKeys[1].Value)
	}

	describeTableReq := new(DescribeTableRequest)
	describeTableReq.TableName = tableName
	_, error := client.DescribeTable(describeTableReq)
	c.Check(error, IsNil)
}

func (s *TableStoreSuite) TestPutGetRow(c *C) {
	log.Println("TestPutGetRow started")
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
	criteria := new(SingleRowQueryCriteria)
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

	sortedColumn, error := mapData.GetRange(2, 2)
	c.Check(error, Equals, nil)
	c.Check(len(sortedColumn), Equals, 2)
	c.Check(sortedColumn[0], Equals, mapData.Columns["col3"][0])
	c.Check(sortedColumn[1], Equals, mapData.Columns["col4"][0])

	mapData2 := getResp.GetColumnMap()
	c.Check(mapData2.Columns["col1"][0].Value, Equals, "col1data1")

	_, error = mapData.GetRange(2, 10)
	c.Check(error, NotNil)
	// Test add column to get
	getRowRequest = new(GetRowRequest)
	criteria = new(SingleRowQueryCriteria)
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
	criteria = new(SingleRowQueryCriteria)
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	_, error = client.GetRow(getRowRequest)
	c.Check(error, NotNil)

	notExistPk := new(PrimaryKey)
	notExistPk.AddPrimaryKeyColumn("pk1", "notexistpk")
	getRowRequest = new(GetRowRequest)
	criteria = new(SingleRowQueryCriteria)

	criteria.PrimaryKey = notExistPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1

	getResp, error = client.GetRow(getRowRequest)
	c.Check(error, IsNil)
	c.Check(getResp, NotNil)

	colmap := getResp.GetColumnMap()
	c.Check(colmap, NotNil)

	log.Println("TestPutGetRow finished")
}

func (s *TableStoreSuite) TestCreateTableAndPutRow(c *C) {
	log.Println("TestCreateTableAndPutRow finished")

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

	log.Println("TestCreateTableAndPutRow finished")
}

func (s *TableStoreSuite) TestPutGetRowWithTimestamp(c *C) {
	log.Println("TestPutGetRowWithTimestamp started")
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", "testtskey1")
	putRowChange.PrimaryKey = putPk
	timeNow := time.Now().Unix() * 1000
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
	criteria := new(SingleRowQueryCriteria)
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
	log.Println("timerange", timeNow)
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Specific: timeNow - 1}
	getResp2, error := client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp2, NotNil)
	c.Check(len(getResp2.PrimaryKey.PrimaryKeys), Equals, 0)

	getRowRequest.SingleRowQueryCriteria.MaxVersion = 0
	log.Println("timerange", timeNow)
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Start: timeNow + 1, End: timeNow + 2}
	getResp2, error = client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp2, NotNil)

	getRowRequest.SingleRowQueryCriteria.MaxVersion = 0
	log.Println("timerange", timeNow)
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Specific: timeNow - 1}
	getResp2, error = client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp2, NotNil)
	c.Check(len(getResp2.PrimaryKey.PrimaryKeys), Equals, 0)

	log.Println("timerange", timeNow)
	getRowRequest.SingleRowQueryCriteria.AddColumnToGet("col1")
	getRowRequest.SingleRowQueryCriteria.TimeRange = &TimeRange{Start: timeNow - 1, End: timeNow + 2}
	getResp2, error = client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp2, NotNil)
	c.Check(len(getResp2.PrimaryKey.PrimaryKeys), Equals, 1)

	log.Println("TestPutGetRowWithTimestamp finished")
}

func (s *TableStoreSuite) TestPutGetRowWithFilter(c *C) {
	log.Println("TestPutGetRowWithFilter started")
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
	criteria := new(SingleRowQueryCriteria)
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
	criteria = new(SingleRowQueryCriteria)
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
	criteria = new(SingleRowQueryCriteria)
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
	log.Println("TestPutGetRowWithFilter finished")
}

func (s *TableStoreSuite) TestPutUpdateDeleteRow(c *C) {
	log.Println("TestPutUpdateDeleteRow started")
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
	criteria := new(SingleRowQueryCriteria)
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
	log.Println(resp.ConsumedCapacityUnit.Write)
	log.Println(resp.ConsumedCapacityUnit.Read)

	_, error = invalidClient.UpdateRow(updateRowRequest)
	c.Check(error, NotNil)

	_, error = invalidClient.DeleteRow(deleteRowReq)
	c.Check(error, NotNil)

	log.Println("TestPutUpdateDeleteRow finished")
}

func (s *TableStoreSuite) TestBatchGetRow(c *C) {
	log.Println("TestBatchGetRow started")
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

	for index, rowToCheck := range batchGetResponse.TableToRowsResult[mqCriteria.TableName] {
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
	mqCriteria.TimeRange = &TimeRange{Start: timeNow - 10000, End: timeNow + 10000}
	mqCriteria.TableName = defaultTableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, error = client.BatchGetRow(batchGetReq)
	c.Check(error, Equals, nil)
	c.Check(len(batchGetResponse.TableToRowsResult), Equals, 1)
	c.Check(len(batchGetResponse.TableToRowsResult[mqCriteria.TableName]), Equals, rowCount)

	for index, rowToCheck := range batchGetResponse.TableToRowsResult[mqCriteria.TableName] {
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
	mqCriteria.TimeRange = &TimeRange{Start: timeNow + 10000, End: timeNow + 20000}
	mqCriteria.TableName = defaultTableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, error = client.BatchGetRow(batchGetReq)
	c.Check(error, Equals, nil)

	c.Check(len(batchGetResponse.TableToRowsResult), Equals, 1)
	c.Check(len(batchGetResponse.TableToRowsResult[mqCriteria.TableName]), Equals, rowCount)

	for index, rowToCheck := range batchGetResponse.TableToRowsResult[mqCriteria.TableName] {
		c.Check(rowToCheck.TableName, Equals, mqCriteria.TableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
		c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 1)
		c.Check(len(rowToCheck.Columns), Equals, 0)
		c.Check(rowToCheck.Index, Equals, int32(index))
	}
	_, error = invalidClient.BatchGetRow(batchGetReq)
	c.Check(error, NotNil)

	log.Println("TestBatchGetRow started")
}

func (s *TableStoreSuite) TestBatchGetRowWithFilter(c *C) {
	log.Println("TestBatchGetRowWithFilter started")
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

	for index, rowToCheck := range batchGetResponse.TableToRowsResult[mqCriteria.TableName] {
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

	count := 0
	for index, rowToCheck := range batchGetResponse.TableToRowsResult[mqCriteria.TableName] {
		c.Check(rowToCheck.Index, Equals, int32(index))
		c.Check(rowToCheck.TableName, Equals, mqCriteria.TableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)

		if len(rowToCheck.PrimaryKey.PrimaryKeys) > 0 {
			c.Check(len(rowToCheck.Columns), Equals, 3)
			count++
		}
	}
	c.Check(count, Equals, 1)

	log.Println("TestBatchGetRowWithFilter finished")
}

func (s *TableStoreSuite) TestBatchWriteRow(c *C) {
	log.Println("TestBatchWriteRow started")

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

	for index, rowToCheck := range batchWriteResponse.TableToRowsResult[defaultTableName] {
		c.Check(rowToCheck.Index, Equals, int32(index))
		c.Check(rowToCheck.TableName, Equals, defaultTableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
	}

	_, error = invalidClient.BatchWriteRow(batchWriteReq)
	c.Check(error, NotNil)

	log.Println("TestBatchWriteRow finished")
}

func (s *TableStoreSuite) TestBatchWriteRowReturnPK(c *C) {
	log.Println("TestBatchWriteRowReturnPK started")

	PrepareDataInDefaultTable("updateinbatchkey1", "updateinput1")
	PrepareDataInDefaultTable("deleteinbatchkey1", "deleteinput1")
	batchWriteReq := &BatchWriteRowRequest{}

	putRowReturnPK := CreatePutRowChange("putinbatchkey1", "datainput1")
	putRowReturnPK.SetReturnPk()
	putRowNoReturnPK := CreatePutRowChange("putinbatchkey2", "datainput2")

	deleteRowChange := new(DeleteRowChange)
	deleteRowChange.TableName = defaultTableName
	deletePk := new(PrimaryKey)
	deletePk.AddPrimaryKeyColumn("pk1", "deleteinbatchkey1")
	deleteRowChange.PrimaryKey = deletePk
	deleteRowChange.SetCondition(RowExistenceExpectation_EXPECT_EXIST)

	batchWriteReq.AddRowChange(putRowReturnPK)
	batchWriteReq.AddRowChange(putRowNoReturnPK)
	batchWriteReq.AddRowChange(deleteRowChange)

	batchWriteResponse, error := client.BatchWriteRow(batchWriteReq)
	c.Check(error, Equals, nil)
	c.Check(len(batchWriteResponse.TableToRowsResult), Equals, 1)

	for index, rowToCheck := range batchWriteResponse.TableToRowsResult[defaultTableName] {
		c.Check(rowToCheck.Index, Equals, int32(index))
		c.Check(rowToCheck.TableName, Equals, defaultTableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)

		switch index {
		case 0:
			c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 1)
			c.Check(rowToCheck.PrimaryKey.PrimaryKeys[0].Value, Equals, "putinbatchkey1")
		case 1:
			c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 0)
		case 2:
			c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 0)
		}
	}

	log.Println("TestBatchWriteRowReturnPK finished")

}

func (s *TableStoreSuite) TestBatchWriteRowReturnColumn(c *C) {
	log.Println("TestBatchWriteRowReturnColumn started")

	PrepareValueInDefaultTable("updateinbatchkey3", 64)

	updateRowReturnColumn := new(UpdateRowChange)
	updateRowReturnColumn.TableName = defaultTableName
	updatePk2 := new(PrimaryKey)
	updatePk2.AddPrimaryKeyColumn("pk1", "updateinbatchkey3")
	updateRowReturnColumn.PrimaryKey = updatePk2
	updateRowReturnColumn.IncrementColumn("col1", int64(40))
	updateRowReturnColumn.SetCondition(RowExistenceExpectation_IGNORE)
	updateRowReturnColumn.SetReturnIncrementValue()
	updateRowReturnColumn.AppendIncrementColumnToReturn("col1")

	batchWriteReq := &BatchWriteRowRequest{}
	batchWriteReq.AddRowChange(updateRowReturnColumn)
	batchWriteResponse, error := client.BatchWriteRow(batchWriteReq)
	c.Check(error, Equals, nil)
	c.Check(len(batchWriteResponse.TableToRowsResult), Equals, 1)

	for _, rowToCheck := range batchWriteResponse.TableToRowsResult[defaultTableName] {
		c.Check(rowToCheck.Columns[0].Value, Equals, int64(104))
	}

	log.Println("TestBatchWriteRowReturnColumn finished")
}

func (s *TableStoreSuite) TestAtomicBatchWriteRowWithSamePartitionKey(c *C) {
	log.Println("TestAtomicBatchWriteRowWithSamePartitionKey started")

	batchWriteReq := &BatchWriteRowRequest{}
	batchWriteReq.IsAtomic = true

	for i := 0; i < 10; i++ {
		rowChange := CreatePutRowChangeV2("atomicPk1", "atomicPk2_"+strconv.Itoa(i), "colVal1_"+strconv.Itoa(i), rangeQueryTableName)
		batchWriteReq.AddRowChange(rowChange)
	}

	updateRowChange := new(UpdateRowChange)
	updateRowChange.TableName = rangeQueryTableName
	updatePk := new(PrimaryKey)
	updatePk.AddPrimaryKeyColumn("pk1", "atomicPk1")
	updatePk.AddPrimaryKeyColumn("pk2", "atomicPk2_0")
	updateRowChange.PrimaryKey = updatePk
	updateRowChange.DeleteColumn("col1")
	updateRowChange.PutColumn("col2", int64(77))
	updateRowChange.PutColumn("col3", "newcol3")
	updateRowChange.SetCondition(RowExistenceExpectation_IGNORE)

	deleteRowChange := new(DeleteRowChange)
	deleteRowChange.TableName = rangeQueryTableName
	deletePk := new(PrimaryKey)
	deletePk.AddPrimaryKeyColumn("pk1", "atomicPk1")
	deletePk.AddPrimaryKeyColumn("pk2", "atomicPk2_1")
	deleteRowChange.PrimaryKey = deletePk
	deleteRowChange.SetCondition(RowExistenceExpectation_IGNORE)

	batchWriteReq.AddRowChange(updateRowChange)
	batchWriteReq.AddRowChange(deleteRowChange)

	batchWriteResponse, error := client.BatchWriteRow(batchWriteReq)
	c.Check(error, Equals, nil)
	c.Check(len(batchWriteResponse.TableToRowsResult), Equals, 1)

	for index, rowToCheck := range batchWriteResponse.TableToRowsResult[rangeQueryTableName] {
		c.Check(rowToCheck.Index, Equals, int32(index))
		c.Check(rowToCheck.TableName, Equals, rangeQueryTableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
	}

	_, error = invalidClient.BatchWriteRow(batchWriteReq)
	c.Check(error, NotNil)

	log.Println("TestAtomicBatchWriteRowWithSamePartitionKey finished")
}

func (s *TableStoreSuite) TestAtomicBatchWriteRowWithDiffPartitionKey(c *C) {
	log.Println("TestAtomicBatchWriteRowWithDiffPartitionKey started")

	batchWriteReq := &BatchWriteRowRequest{}
	batchWriteReq.IsAtomic = true

	for i := 0; i < 10; i++ {
		rowChange := CreatePutRowChangeV2("atomicPk1", "atomicPk2_"+strconv.Itoa(i), "colVal1_"+strconv.Itoa(i), rangeQueryTableName)
		batchWriteReq.AddRowChange(rowChange)
	}
	rowChange := CreatePutRowChangeV2("atomicPk1_1", "atomicPk2_"+strconv.Itoa(10), "colVal1_"+strconv.Itoa(10), rangeQueryTableName)
	batchWriteReq.AddRowChange(rowChange)

	batchWriteResponse, error := client.BatchWriteRow(batchWriteReq)
	c.Check(error, Equals, nil)
	c.Check(len(batchWriteResponse.TableToRowsResult), Equals, 1)

	for index, rowToCheck := range batchWriteResponse.TableToRowsResult[rangeQueryTableName] {
		c.Check(rowToCheck.Index, Equals, int32(index))
		c.Check(rowToCheck.TableName, Equals, rangeQueryTableName)
		c.Check(rowToCheck.IsSucceed, Equals, false)
	}

	_, error = invalidClient.BatchWriteRow(batchWriteReq)
	c.Check(error, NotNil)

	log.Println("TestAtomicBatchWriteRowWithDiffPartitionKey finished")
}

func (s *TableStoreSuite) TestFuzzyGetRangeMatrix(c *C) {
	log.Println("TestFuzzyGetRangeMatrix started")
	expect, err := PrepareFuzzyTableData(fuzzyTableName, 1024, 10240, 10000)
	c.Assert(err, IsNil)
	startPk := new(PrimaryKey)
	startPk.AddPrimaryKeyColumnWithMinValue(fuzzyMetaPk1)
	startPk.AddPrimaryKeyColumnWithMinValue(fuzzyMetaPk2)
	startPk.AddPrimaryKeyColumnWithMinValue(fuzzyMetaPk3)
	endPk := new(PrimaryKey)
	endPk.AddPrimaryKeyColumnWithMaxValue(fuzzyMetaPk1)
	endPk.AddPrimaryKeyColumnWithMaxValue(fuzzyMetaPk2)
	endPk.AddPrimaryKeyColumnWithMaxValue(fuzzyMetaPk3)
	criteria := &RangeRowQueryCriteria{
		TableName:       fuzzyTableName,
		StartPrimaryKey: startPk,
		EndPrimaryKey:   endPk,
		ColumnsToGet:    fuzzyMetaAttr,
		MaxVersion:      1,
		Limit:           2000,
		DataBlockType:   SimpleRowMatrix,
	}
	for {
		resp, err := client.GetRange(&GetRangeRequest{criteria})
		c.Assert(err, IsNil)
		c.Assert(resp.DataBlockType, Equals, SimpleRowMatrix)
		for _, row := range resp.Rows {
			pks := row.PrimaryKey.PrimaryKeys
			cpKey := pks[0].Value.(string) + "-" + base64.StdEncoding.EncodeToString(pks[1].Value.([]byte)) +
				"-" + fmt.Sprintf("%d", pks[2].Value.(int64))
			attrs, ok := expect[cpKey]
			if !ok {
				c.Errorf("got %s", cpKey)
			}
			for _, column := range row.Columns {
				//log.Printf("%s %v\n", column.ColumnName, column.Value)
				switch column.ColumnName {
				case "string":
					c.Assert(column.Value, DeepEquals, attrs[column.ColumnName])
				case "blob":
					c.Assert(column.Value.([]byte), DeepEquals, attrs[column.ColumnName].([]byte))
				case "boolean":
					c.Assert(column.Value, DeepEquals, attrs[column.ColumnName])
				case "integer":
					c.Assert(column.Value, DeepEquals, attrs[column.ColumnName])
				case "double":
					c.Assert(column.Value, DeepEquals, attrs[column.ColumnName])
				}
			}
			c.Assert(len(row.Columns), Equals, len(attrs))
			delete(expect, cpKey)
		}
		if resp.NextStartPrimaryKey != nil {
			criteria.StartPrimaryKey = resp.NextStartPrimaryKey
		} else {
			break
		}
	}
	c.Assert(len(expect), Equals, 0)
	log.Println("TestFuzzyGetRangeMatrix finished")
}

func PrepareFuzzyTableData(tableName string, maxStringLen, maxBlobLen int, rowCount int) (map[string]map[string]interface{}, error) {
	retMap := make(map[string]map[string]interface{})
	for i := 0; i < rowCount; i++ {
		pk := new(PrimaryKey)
		pkStr := randStringRunes(128)
		pk.AddPrimaryKeyColumn(fuzzyMetaPk1, pkStr)
		buf := make([]byte, rand.Intn(512))
		rand.Read(buf)
		pk.AddPrimaryKeyColumn(fuzzyMetaPk2, buf)
		pkInt := rand.Int63()
		pk.AddPrimaryKeyColumn(fuzzyMetaPk3, pkInt)
		encodeKey := pkStr + "-" + base64.StdEncoding.EncodeToString(buf) + "-" + fmt.Sprintf("%d", pkInt)
		change := &PutRowChange{
			TableName:  tableName,
			PrimaryKey: pk,
			Condition:  &RowCondition{RowExistenceExpectation: RowExistenceExpectation_IGNORE},
		}
		attrs := make(map[string]interface{}, len(fuzzyMetaAttr))
		for _, nameType := range fuzzyMetaAttr {
			var v interface{}
			switch nameType {
			case "string":
				v = randStringRunes(maxStringLen)
			case "integer":
				v = rand.Int63n(101)
				if v.(int64)%10 == 0 {
					v = nil
				}
			case "boolean":
				v = rand.Int()%2 == 0
			case "double":
				v = rand.Float64()
			case "blob":
				v = make([]byte, rand.Intn(maxBlobLen)+1)
				rand.Read(v.([]byte))
			}
			if v != nil {
				change.AddColumn(nameType, v)
				attrs[nameType] = v
			}
		}
		_, err := client.PutRow(&PutRowRequest{change})
		if err != nil {
			return nil, err
		}
		retMap[encodeKey] = attrs
	}
	return retMap, nil
}

func (s *TableStoreSuite) TestGetRange(c *C) {
	log.Println("TestGetRange started")
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
	startPK.AddPrimaryKeyColumn("pk1", "getrange"+strconv.Itoa(start))
	endPK := new(PrimaryKey)
	endPK.AddPrimaryKeyColumn("pk1", "getrange"+strconv.Itoa(end))
	rangeRowQueryCriteria.StartPrimaryKey = startPK
	rangeRowQueryCriteria.EndPrimaryKey = endPK
	rangeRowQueryCriteria.Direction = FORWARD
	rangeRowQueryCriteria.MaxVersion = 1
	rangeRowQueryCriteria.ColumnsToGet = []string{"col1"}
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria

	log.Println("check", rangeRowQueryCriteria.ColumnsToGet)
	log.Println("check2", getRangeRequest.RangeRowQueryCriteria.ColumnsToGet)
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

	log.Println("use time range to query rows")

	rangeRowQueryCriteria.TimeRange = &TimeRange{Specific: timeNow - 100001}
	getRangeResp, error = client.GetRange(getRangeRequest)
	c.Check(error, NotNil)
	log.Println(error)

	log.Println("use time range to query rows 2")
	rangeRowQueryCriteria.TimeRange = &TimeRange{Start: timeNow + 1, End: timeNow + 2}
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria
	getRangeResp2, error := client.GetRange(getRangeRequest)

	c.Check(error, Equals, nil)
	c.Check(getRangeResp2.Rows, NotNil)
	c.Check(len(getRangeResp2.Rows), Equals, count)
	c.Check(len(getRangeResp2.Rows[0].Columns), Equals, 0)

	_, error = invalidClient.GetRange(getRangeRequest)
	c.Check(error, NotNil)
	log.Println("TestGetRange finished")
}

func (s *TableStoreSuite) TestGetRangeWithPagination(c *C) {
	log.Println("TestGetRangeWithPagination started")
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
	startPK.AddPrimaryKeyColumn("pk1", "testrangequery"+strconv.Itoa(start))
	endPK := new(PrimaryKey)
	endPK.AddPrimaryKeyColumn("pk1", "testrangequery"+strconv.Itoa(end))
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
	log.Println("TestGetRangeWithPagination finished")
}

func (s *TableStoreSuite) TestGetRangeWithFilter(c *C) {
	log.Println("TestGetRange started")
	rowCount := 20
	timeNow := time.Now().Unix() * 1000
	for i := 0; i < rowCount; i++ {
		key := "zgetrangetest" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		PrepareDataInRangeTableWithTimestamp("pk1", key, value, timeNow)
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
	filter1 := NewSingleColumnCondition("pk2", ComparatorType(CT_GREATER_EQUAL), "pk3")
	filter2 := NewSingleColumnCondition("pk2", ComparatorType(CT_LESS_EQUAL), "pk3")
	filter.AddFilter(filter2)
	filter.AddFilter(filter1)
	rangeRowQueryCriteria.Filter = filter
	getRangeRequest.RangeRowQueryCriteria = rangeRowQueryCriteria

	getRangeResp, error := client.GetRange(getRangeRequest)
	c.Check(error, Equals, nil)
	log.Println(getRangeResp)
	log.Println(getRangeResp.NextStartPrimaryKey)
	log.Println(getRangeResp.Rows)
	//log.Println(getRangeResp.NextStartPrimaryKey)
	//c.Check(getRangeResp.Rows, NotNil)

	log.Println("TestGetRange with filter finished")
}

func (s *TableStoreSuite) TestGetRangeWithMinMaxValue(c *C) {
	log.Println("TestGetRangeWithMinMaxValue started")

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
	log.Println("TestGetRangeWithMinMaxValue finished")
}

func (s *TableStoreSuite) TestPutRowsWorkload(c *C) {
	log.Println("TestPutRowsWorkload started")

	start := time.Now().UnixNano()

	isFinished := make(chan bool)
	totalCount := 100
	for i := 0; i < totalCount; i++ {
		value := i * 10000
		go func(index int) {
			for j := 0; j < 100; j++ {
				currentIndex := index + j
				rowToPut1 := CreatePutRowChange("workloadtestkey"+strconv.Itoa(currentIndex), "perfdata1")
				putRowRequest := new(PutRowRequest)
				putRowRequest.PutRowChange = rowToPut1
				_, error := client.PutRow(putRowRequest)
				if error != nil {
					log.Println("put row error", error)
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
		log.Println("catched count is:", count)
		if count >= totalCount {
			close(isFinished)
		}
	}
	c.Check(count, Equals, totalCount)
	end := time.Now().UnixNano()

	totalCost := (end - start) / 1000000
	log.Println("total cost:", totalCost)
	c.Check(totalCost < 30*1000, Equals, true)

	time.Sleep(time.Millisecond * 20)
	log.Println("TestPutRowsWorkload finished")
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
	tableMeta.AddPrimaryKeyColumn("pk6", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk7", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk8", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk9", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk10", PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn("pk11", PrimaryKeyType_STRING)

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
	_, err = client.DeleteTable(deleteReq)
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
	log.Println("TestMockHttpClientCase started")
	currentGetHttpClientFunc = func() IHttpClient {
		return &mockHttpClient{}
	}

	tempClient := NewClientWithConfig("test", "a", "b", "c", "d", NewDefaultTableStoreConfig())
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
	data := tempClient.httpClient.(*mockHttpClient)

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

	log.Println("TestMockHttpClientCase finished")
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

	tempClient := NewClient("a", "b", "c", "d", SetSth())
	c.Check(tempClient, NotNil)
	config := NewDefaultTableStoreConfig()
	tempClient = NewClientWithConfig("a", "b", "c", "d", "e", config)
	c.Check(tempClient, NotNil)

	errorCode := INTERNAL_SERVER_ERROR
	tsClient := client.(*TableStoreClient)
	value := tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 10, time.Now().Add(time.Second*1), 10, getRowUri)
	c.Check(value == 0, Equals, true)

	errorCode = ROW_OPERATION_CONFLICT
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), 10, getRowUri)
	c.Check(value > 0, Equals, true)

	errorCode = STORAGE_TIMEOUT
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), 10, putRowUri)
	c.Check(value == 0, Equals, true)

	errorCode = STORAGE_TIMEOUT
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), 10, getRowUri)
	c.Check(value > 0, Equals, true)

	errorCode = STORAGE_TIMEOUT
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), MaxRetryInterval, getRowUri)
	c.Check(value == MaxRetryInterval, Equals, true)

	// stream api
	errorCode = STORAGE_TIMEOUT
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), 10, getStreamRecordUri)
	c.Check(value > 0, Equals, true)

	// 502
	errorCode = SERVER_UNAVAILABLE
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: "bad gateway"}, 1, time.Now().Add(time.Second*1), 10, getStreamRecordUri)
	c.Check(value > 0, Equals, true)

	// 502 write
	errorCode = SERVER_UNAVAILABLE
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: "bad gateway"}, 1, time.Now().Add(time.Second*1), 10, putRowUri)
	c.Check(value == 0, Equals, true)

	// 400 normal
	errorCode = "OTSPermissionDenied"
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), 10, putRowUri)
	c.Check(value == 0, Equals, true)

	// 400 raw http
	errorCode = OTS_CLIENT_UNKNOWN
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), 10, getRowUri)
	c.Check(value == 0, Equals, true)

	// storage 503 put
	errorCode = STORAGE_SERVER_BUSY
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), 10, putRowUri)
	c.Check(value > 0, Equals, true)

	// storage 503 desc stream
	errorCode = STORAGE_SERVER_BUSY
	value = tsClient.getNextPause(&OtsError{Code: errorCode, Message: errorCode}, 1, time.Now().Add(time.Second*1), 10, describeStreamUri)
	c.Check(value > 0, Equals, true)

	// EOF
	value = tsClient.getNextPause(io.EOF, 1, time.Now().Add(time.Second*1), 10, putRowUri)
	c.Check(value > 0, Equals, true)

	// connection rest
	value = tsClient.getNextPause(syscall.ECONNRESET, 1, time.Now().Add(time.Second*1), 10, putRowUri)
	c.Check(value > 0, Equals, true)

	getResp := &GetRowResponse{}
	colMap := getResp.GetColumnMap()
	c.Check(colMap, NotNil)

	getResp = &GetRowResponse{}
	col1 := &AttributeColumn{ColumnName: "col1", Value: "value1"}
	col2 := &AttributeColumn{ColumnName: "col1", Value: "value2"}
	col3 := &AttributeColumn{ColumnName: "col2", Value: "value3"}

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

func SetSth() ClientOption {
	return func(client *TableStoreClient) {
		log.Println(client.accessKeyId)
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

func CreatePutRowChangeV2(pk1Value, pk2Value, colValue, tableName string) *PutRowChange {
	putRowChange := new(PutRowChange)
	putRowChange.TableName = tableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", pk1Value)
	putPk.AddPrimaryKeyColumn("pk2", pk2Value)
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", colValue)
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	return putRowChange
}

type mockHttpClient struct {
	response   *http.Response
	error      error
	httpClient *http.Client
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

func PrepareValueInDefaultTable(key string, value int64) error {
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
	putRowChange.AddColumnWithTimestamp("col2", value, timeNow)
	putRowChange.AddColumnWithTimestamp("col3", value, timeNow)
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

func (s *TableStoreSuite) TestListStream(c *C) {
	tableName := defaultTableName + "_ListStream"
	log.Printf("TestListStream starts on table %s\n", tableName)
	{
		err := PrepareTable(tableName)
		c.Assert(err, IsNil)
	}
	defer client.DeleteTable(&DeleteTableRequest{TableName: tableName})
	{
		resp, err := client.DescribeTable(&DescribeTableRequest{TableName: tableName})
		c.Assert(err, IsNil)
		c.Assert(resp.StreamDetails, NotNil)
		c.Assert(resp.StreamDetails.EnableStream, Equals, false)
		c.Assert(resp.StreamDetails.StreamId, IsNil)
		c.Assert(resp.StreamDetails.ExpirationTime, Equals, int32(0))
		c.Assert(resp.StreamDetails.LastEnableTime, Equals, int64(0))
	}
	{
		resp, err := client.ListStream(&ListStreamRequest{TableName: &tableName})
		c.Assert(err, IsNil)
		log.Printf("%v\n", resp)
		c.Assert(len(resp.Streams), Equals, 0)
	}
	{
		resp, err := client.UpdateTable(&UpdateTableRequest{
			TableName:  tableName,
			StreamSpec: &StreamSpecification{EnableStream: true, ExpirationTime: 24}})
		c.Assert(err, IsNil)
		c.Assert(resp.StreamDetails, NotNil)
	}
	{
		resp, err := client.ListStream(&ListStreamRequest{TableName: &tableName})
		c.Assert(err, IsNil)
		log.Printf("%#v\n", resp)
		c.Assert(len(resp.Streams), Equals, 1)
	}
	{
		resp, err := client.DescribeTable(&DescribeTableRequest{TableName: tableName})
		c.Assert(err, IsNil)
		c.Assert(resp.StreamDetails, NotNil)
		log.Printf("%#v\n", resp)
		c.Assert(resp.StreamDetails.EnableStream, Equals, true)
		c.Assert(resp.StreamDetails.StreamId, NotNil)
		c.Assert(resp.StreamDetails.ExpirationTime, Equals, int32(24))
		c.Assert(resp.StreamDetails.LastEnableTime > 0, Equals, true)
	}
	log.Println("TestListStream finish")
}

func (s *TableStoreSuite) TestCreateTableWithStream(c *C) {
	tableName := defaultTableName + "_CreateTableWithStream"
	log.Printf("TestCreateTableWithStream starts on table %s\n", tableName)
	{
		req := CreateTableRequest{}
		tableMeta := TableMeta{}
		tableMeta.TableName = tableName
		tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
		req.TableMeta = &tableMeta

		tableOption := TableOption{}
		tableOption.TimeToAlive = -1
		tableOption.MaxVersion = 3
		req.TableOption = &tableOption

		req.ReservedThroughput = &ReservedThroughput{Readcap: 0, Writecap: 0}

		req.StreamSpec = &StreamSpecification{EnableStream: true, ExpirationTime: 24}

		_, err := client.CreateTable(&req)
		c.Assert(err, IsNil)
	}
	defer client.DeleteTable(&DeleteTableRequest{TableName: tableName})
	{
		resp, err := client.ListStream(&ListStreamRequest{TableName: &tableName})
		c.Assert(err, IsNil)
		log.Printf("%#v\n", resp)
		c.Assert(len(resp.Streams), Equals, 1)
	}
	log.Println("TestCreateTableWithStream finish")
}

func (s *TableStoreSuite) TestStream(c *C) {
	tableName := defaultTableName + "_Stream"
	log.Printf("TestCreateTableWithStream starts on table %s\n", tableName)
	{
		req := CreateTableRequest{}
		tableMeta := TableMeta{}
		tableMeta.TableName = tableName
		tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
		req.TableMeta = &tableMeta

		tableOption := TableOption{}
		tableOption.TimeToAlive = -1
		tableOption.MaxVersion = 3
		req.TableOption = &tableOption

		req.ReservedThroughput = &ReservedThroughput{Readcap: 0, Writecap: 0}

		req.StreamSpec = &StreamSpecification{EnableStream: true, ExpirationTime: 24}

		_, err := client.CreateTable(&req)
		c.Assert(err, IsNil)
	}
	defer client.DeleteTable(&DeleteTableRequest{TableName: tableName})
	var streamId *StreamId
	{
		resp, err := client.ListStream(&ListStreamRequest{TableName: &tableName})
		c.Assert(err, IsNil)
		log.Printf("%#v\n", resp)
		c.Assert(len(resp.Streams), Equals, 1)
		streamId = resp.Streams[0].Id
	}
	c.Assert(streamId, NotNil)
	var shardId *ShardId
	for {
		resp, err := client.DescribeStream(&DescribeStreamRequest{StreamId: streamId})
		c.Assert(err, IsNil)
		log.Printf("DescribeStreamResponse: %#v\n", resp)
		c.Assert(*resp.StreamId, Equals, *streamId)
		c.Assert(resp.ExpirationTime, Equals, int32(24))
		c.Assert(*resp.TableName, Equals, tableName)
		c.Assert(len(resp.Shards), Equals, 1)
		log.Printf("StreamShard: %#v\n", resp.Shards[0])
		shardId = resp.Shards[0].SelfShard
		if resp.Status == SS_Active {
			break
		}
	}
	c.Assert(shardId, NotNil)
	var iter *ShardIterator
	var records []*StreamRecord
	{
		resp, err := client.GetShardIterator(&GetShardIteratorRequest{
			StreamId: streamId,
			ShardId:  shardId})
		c.Assert(err, IsNil)
		c.Assert(resp.ShardIterator, NotNil)
		iter = resp.ShardIterator
	}
	log.Printf("init iterator: %#v\n", *iter)
	iter, _ = exhaustStreamRecords(c, iter)
	log.Printf("put row:\n")
	{
		req := PutRowRequest{}
		rowChange := PutRowChange{}
		rowChange.TableName = tableName
		pk := PrimaryKey{}
		pk.AddPrimaryKeyColumn("pk1", "rowkey")
		rowChange.PrimaryKey = &pk
		rowChange.AddColumn("colToDel", "abc")
		rowChange.AddColumn("colToDelAll", true)
		rowChange.AddColumn("colToUpdate", int64(123))
		rowChange.SetCondition(RowExistenceExpectation_IGNORE)
		req.PutRowChange = &rowChange
		_, err := client.PutRow(&req)
		c.Assert(err, IsNil)
	}
	iter, records = exhaustStreamRecords(c, iter)
	var timestamp int64
	{
		c.Assert(len(records), Equals, 1)
		r := records[0]
		c.Assert(r.Type, Equals, AT_Put)
		c.Assert(r.Info, NotNil)
		c.Assert(r.PrimaryKey, NotNil)

		pkey := r.PrimaryKey
		c.Assert(len(pkey.PrimaryKeys), Equals, 1)
		pkc := pkey.PrimaryKeys[0]
		c.Assert(pkc, NotNil)
		c.Assert(pkc.ColumnName, Equals, "pk1")
		c.Assert(pkc.Value, Equals, "rowkey")
		c.Assert(pkc.PrimaryKeyOption, Equals, NONE)

		c.Assert(len(r.Columns), Equals, 3)
		attr0 := r.Columns[0]
		attr1 := r.Columns[1]
		attr2 := r.Columns[2]
		c.Assert(attr0, NotNil)
		c.Assert(*attr0.Name, Equals, "colToDel")
		c.Assert(attr0.Type, Equals, RCT_Put)
		c.Assert(attr0.Value, Equals, "abc")
		c.Assert(attr1, NotNil)
		c.Assert(*attr1.Name, Equals, "colToDelAll")
		c.Assert(attr1.Type, Equals, RCT_Put)
		c.Assert(attr1.Value, Equals, true)
		timestamp = *attr0.Timestamp
		c.Assert(attr2, NotNil)
		c.Assert(*attr2.Name, Equals, "colToUpdate")
		c.Assert(attr2.Type, Equals, RCT_Put)
		c.Assert(attr2.Value, Equals, int64(123))
	}
	{
		chg := UpdateRowChange{}
		chg.TableName = tableName
		pk := PrimaryKey{}
		pk.AddPrimaryKeyColumn("pk1", "rowkey")
		chg.PrimaryKey = &pk
		chg.SetCondition(RowExistenceExpectation_IGNORE)
		chg.DeleteColumnWithTimestamp("colToDel", timestamp)
		chg.DeleteColumn("colToDelAll")
		chg.PutColumn("colToUpdate", 3.14)
		_, err := client.UpdateRow(&UpdateRowRequest{UpdateRowChange: &chg})
		c.Assert(err, IsNil)
	}
	iter, records = exhaustStreamRecords(c, iter)
	{
		c.Assert(len(records), Equals, 1)
		r := records[0]
		c.Assert(r.Type, Equals, AT_Update)
		c.Assert(r.Info, NotNil)
		c.Assert(r.PrimaryKey, NotNil)

		pkey := r.PrimaryKey
		c.Assert(len(pkey.PrimaryKeys), Equals, 1)
		pkc := pkey.PrimaryKeys[0]
		c.Assert(pkc, NotNil)
		c.Assert(pkc.ColumnName, Equals, "pk1")
		c.Assert(pkc.Value, Equals, "rowkey")
		c.Assert(pkc.PrimaryKeyOption, Equals, NONE)

		c.Assert(len(r.Columns), Equals, 3)
		attr0 := r.Columns[0]
		attr1 := r.Columns[1]
		attr2 := r.Columns[2]
		c.Assert(attr0, NotNil)
		c.Assert(*attr0.Name, Equals, "colToDel")
		c.Assert(attr0.Type, Equals, RCT_DeleteOneVersion)
		c.Assert(attr0.Value, IsNil)
		c.Assert(attr0.Timestamp, NotNil)
		c.Assert(*attr0.Timestamp, Equals, timestamp)
		c.Assert(attr1, NotNil)
		c.Assert(*attr1.Name, Equals, "colToDelAll")
		c.Assert(attr1.Type, Equals, RCT_DeleteAllVersions)
		c.Assert(attr1.Value, IsNil)
		c.Assert(attr1.Timestamp, IsNil)
		c.Assert(attr2, NotNil)
		c.Assert(*attr2.Name, Equals, "colToUpdate")
		c.Assert(attr2.Type, Equals, RCT_Put)
		c.Assert(attr2.Value, Equals, 3.14)
	}
	{
		chg := DeleteRowChange{}
		chg.TableName = tableName
		pk := PrimaryKey{}
		pk.AddPrimaryKeyColumn("pk1", "rowkey")
		chg.PrimaryKey = &pk
		chg.SetCondition(RowExistenceExpectation_IGNORE)
		_, err := client.DeleteRow(&DeleteRowRequest{DeleteRowChange: &chg})
		c.Assert(err, IsNil)
	}
	iter, records = exhaustStreamRecords(c, iter)
	{
		c.Assert(len(records), Equals, 1)
		r := records[0]
		c.Assert(r.Type, Equals, AT_Delete)
		c.Assert(r.Info, NotNil)
		c.Assert(r.PrimaryKey, NotNil)

		pkey := r.PrimaryKey
		c.Assert(len(pkey.PrimaryKeys), Equals, 1)
		pkc := pkey.PrimaryKeys[0]
		c.Assert(pkc, NotNil)
		c.Assert(pkc.ColumnName, Equals, "pk1")
		c.Assert(pkc.Value, Equals, "rowkey")
		c.Assert(pkc.PrimaryKeyOption, Equals, NONE)

		c.Assert(len(r.Columns), Equals, 0)
	}
	log.Println("TestCreateTableWithStream finish")
}

func exhaustStreamRecords(c *C, iter *ShardIterator) (*ShardIterator, []*StreamRecord) {
	records := make([]*StreamRecord, 0)
	for {
		resp, err := client.GetStreamRecord(&GetStreamRecordRequest{
			ShardIterator: iter})
		c.Assert(err, IsNil)
		log.Printf("#records: %d\n", len(resp.Records))
		for i, rec := range resp.Records {
			log.Printf("record %d: %s\n", i, rec)
		}
		for _, rec := range resp.Records {
			records = append(records, rec)
		}
		nextIter := resp.NextShardIterator
		if nextIter == nil {
			log.Printf("next iterator: %#v\n", nextIter)
			break
		} else {
			log.Printf("next iterator: %#v\n", *nextIter)
		}
		if *iter == *nextIter {
			break
		}
		iter = nextIter
	}
	return iter, records
}

func (s *TableStoreSuite) TestSQL(c *C) {
	//queries := []string{
	//	"create table if not exists test_http_query (a bigint not null, b double not null, c mediumtext not null, d mediumblob not null, e bool not null, primary key (`a`));",
	//	"insert ignore into test_http_query values(0, 0.0, '0', '0', false);",
	//	"insert ignore into test_http_query values(1, 1.0, '1', '1', true);",
	//	"insert ignore into test_http_query values(2, 2.0, '2', '2', false);",
	//	"insert ignore into test_http_query (a, b, e) values(3, 3.0, true);",
	//}
	//for _, query := range queries {
	//	resp, err := client.SQLQuery(&SQLQueryRequest{Query: query})
	//	c.Assert(err, IsNil)
	//	c.Assert(resp.Rows, IsNil)
	//}
	resp, err := client.SQLQuery(&SQLQueryRequest{
		Query: "create table if not exists test_http_query (a bigint not null, b double not null, c mediumtext not null, d mediumblob not null, e bool not null, primary key (`a`));",
	})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_CREATE_TABLE)
	c.Assert(resp.ResultSet, IsNil)

	resp, err = client.SQLQuery(&SQLQueryRequest{
		Query: "show tables;",
	})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_SHOW_TABLE)
	hasTable := false
	for resp.ResultSet.HasNext() {
		row := resp.ResultSet.Next()
		tblName, err := row.GetString(0)
		c.Assert(err, IsNil)
		if tblName == "test_http_query" {
			hasTable = true
		}
	}
	c.Assert(hasTable, Equals, true)

	resp, err = client.SQLQuery(&SQLQueryRequest{
		Query: "drop mapping table test_http_query;",
	})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_DROP_TABLE)
	c.Assert(resp.ResultSet, IsNil)

	resp, err = client.SQLQuery(&SQLQueryRequest{
		Query: "create table if not exists test_http_query (a bigint not null, b double not null, c mediumtext not null, d mediumblob not null, e bool not null, primary key (`a`));",
	})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_CREATE_TABLE)
	c.Assert(resp.ResultSet, IsNil)

	resp, err = client.SQLQuery(&SQLQueryRequest{
		Query: "desc test_http_query",
	})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_DESCRIBE_TABLE)
	c.Assert(len(resp.ResultSet.Columns()), Equals, 6)
	for resp.ResultSet.HasNext() {
		log.Println(resp.ResultSet.Next().DebugString())
	}

	log.Println("sql query via FLAT_BUFFERS")
	resp, err = client.SQLQuery(&SQLQueryRequest{Query: "select * from test_http_query"})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_SELECT)
	c.Assert(resp.SQLQueryConsumed, NotNil)
	c.Assert(len(resp.SQLQueryConsumed.SearchConsumes), Equals, 0)
	c.Assert(len(resp.SQLQueryConsumed.TableConsumes), Equals, 1)
	c.Assert(resp.SQLQueryConsumed.TableConsumes[0].TableName, Equals, "test_http_query")
	c.Assert(resp.SQLQueryConsumed.TableConsumes[0].ConsumedCapacityUnit.Read > 0, Equals, true)
	resultSet := resp.ResultSet
	c.Assert(len(resultSet.Columns()), Equals, 5)
	i := 0
	for resultSet.HasNext() {
		sqlRow := resultSet.Next()
		log.Println(sqlRow.DebugString())

		val, err := sqlRow.GetInt64(0)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, int64(i))
		val, err = sqlRow.GetInt64ByName("a")
		c.Assert(err, IsNil)
		c.Assert(val, Equals, int64(i))

		_, err = sqlRow.GetFloat64(0)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not DOUBLE")
		_, err = sqlRow.GetBytes(0)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not BINARY")
		_, err = sqlRow.GetBool(0)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not BOOLEAN")
		_, err = sqlRow.GetString(0)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not STRING")

		val2, err := sqlRow.GetFloat64(1)
		c.Assert(err, IsNil)
		c.Assert(val2, Equals, float64(i))
		val2, err = sqlRow.GetFloat64ByName("b")
		c.Assert(err, IsNil)
		c.Assert(val2, Equals, float64(i))
		_, err = sqlRow.GetBytes(1)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not BINARY")
		_, err = sqlRow.GetBool(1)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not BOOLEAN")
		_, err = sqlRow.GetString(1)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not STRING")
		_, err = sqlRow.GetInt64(1)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not INTEGER")

		if i == 3 {
			val, err := sqlRow.IsNull(2)
			c.Assert(err, IsNil)
			c.Assert(val, Equals, true)
			val, err = sqlRow.IsNull(3)
			c.Assert(err, IsNil)
			c.Assert(val, Equals, true)
		} else {
			val, err := sqlRow.GetString(2)
			c.Assert(err, IsNil)
			c.Assert(val, Equals, strconv.Itoa(i))
			val, err = sqlRow.GetStringByName("c")
			c.Assert(err, IsNil)
			c.Assert(val, Equals, strconv.Itoa(i))
			_, err = sqlRow.GetBytes(2)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "the type of column is not BINARY")
			_, err = sqlRow.GetBool(2)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "the type of column is not BOOLEAN")
			_, err = sqlRow.GetFloat64(2)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "the type of column is not DOUBLE")
			_, err = sqlRow.GetInt64(2)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "the type of column is not INTEGER")

			val2, err := sqlRow.GetBytes(3)
			c.Assert(err, IsNil)
			c.Assert(bytes.Equal(val2, []byte(strconv.Itoa(i))), Equals, true)
			val2, err = sqlRow.GetBytesByName("d")
			c.Assert(err, IsNil)
			c.Assert(bytes.Equal(val2, []byte(strconv.Itoa(i))), Equals, true)
			_, err = sqlRow.GetString(3)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "the type of column is not STRING")
			_, err = sqlRow.GetBool(3)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "the type of column is not BOOLEAN")
			_, err = sqlRow.GetFloat64(3)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "the type of column is not DOUBLE")
			_, err = sqlRow.GetInt64(3)
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "the type of column is not INTEGER")
		}

		val4, err := sqlRow.GetBool(4)
		c.Assert(err, IsNil)
		c.Assert(val4, Equals, i%2 == 1)

		// test out of bound
		_, err = sqlRow.GetBool(5)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("colIdx out of bound, max: %d", 4))
		_, err = sqlRow.GetInt64(5)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("colIdx out of bound, max: %d", 4))
		_, err = sqlRow.GetString(5)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("colIdx out of bound, max: %d", 4))
		_, err = sqlRow.GetFloat64(5)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("colIdx out of bound, max: %d", 4))
		_, err = sqlRow.GetBytes(5)
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("colIdx out of bound, max: %d", 4))

		i++
	}
	resp, err = client.SQLQuery(&SQLQueryRequest{Query: "select from_unixtime(1668585138.995),timediff(from_unixtime(1668585138.995),from_unixtime(1668585013.712)),date(from_unixtime(1668585138.995))"})
	c.Assert(err, IsNil)
	timeTpResSet := resp.ResultSet
	for timeTpResSet.HasNext() {
		sqlRow := timeTpResSet.Next()
		val1, err := sqlRow.GetDateTime(0)
		c.Assert(err, IsNil)
		c.Assert(val1, Equals, time.Unix(1668585138, 995000000).UTC())
		val1, err = sqlRow.GetDateTimeByName("from_unixtime(1668585138.995)")
		c.Assert(err, IsNil)
		c.Assert(val1, Equals, time.Unix(1668585138, 995000000).UTC())
		val1, err = sqlRow.GetDateTimeByName("from_unixtime(16685138.995)")
		c.Assert(err.Error(), Equals, "SQLRow doesn't contains Name: from_unixtime(16685138.995)")
		_, err = sqlRow.GetTime(0)
		c.Assert(err.Error(), Equals, "the type of column is not TIME")
		_, err = sqlRow.GetDateTime(5)
		c.Assert(err.Error(), Equals, fmt.Sprintf("colIdx out of bound, max: %d", 2))

		val2, err := sqlRow.GetTime(1)
		c.Assert(err, IsNil)
		c.Assert(val2, Equals, time.Duration(125283000000))
		val2, err = sqlRow.GetTimeByName("timediff(from_unixtime(1668585138.995),from_unixtime(1668585013.712))")
		c.Assert(err, IsNil)
		c.Assert(val2, Equals, time.Duration(125283000000))
		val2, err = sqlRow.GetTimeByName("a")
		c.Assert(err.Error(), Equals, "SQLRow doesn't contains Name: a")
		_, err = sqlRow.GetDateTime(1)
		c.Assert(err.Error(), Equals, "the type of column is not DATETIME")
		_, err = sqlRow.GetDate(1)
		c.Assert(err.Error(), Equals, "the type of column is not DATE")
		_, err = sqlRow.GetTime(5)
		c.Assert(err.Error(), Equals, fmt.Sprintf("colIdx out of bound, max: %d", 2))

		val3, err := sqlRow.GetDate(2)
		c.Assert(err, IsNil)
		c.Assert(val3, Equals, time.Unix(1668556800, 0).UTC())
		val3, err = sqlRow.GetDateByName("date(from_unixtime(1668585138.995))")
		c.Assert(err, IsNil)
		c.Assert(val3, Equals, time.Unix(1668556800, 0).UTC())
		val3, err = sqlRow.GetDateByName("b")
		c.Assert(err.Error(), Equals, "SQLRow doesn't contains Name: b")
		_, err = sqlRow.GetDate(5)
		c.Assert(err.Error(), Equals, fmt.Sprintf("colIdx out of bound, max: %d", 2))
	}
}

func (s *TableStoreSuite) TestSQLWithSearch(c *C) {
	resp, err := client.SQLQuery(&SQLQueryRequest{
		Query: fmt.Sprintf("create table if not exists %s (a bigint not null, b double not null, c mediumtext not null, e bool not null, primary key (`a`));", sqlTableNameWithSearch),
	})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_CREATE_TABLE)
	c.Assert(resp.ResultSet, IsNil)

	resp, err = client.SQLQuery(&SQLQueryRequest{
		Query: "show tables;",
	})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_SHOW_TABLE)
	hasTable := false
	for resp.ResultSet.HasNext() {
		row := resp.ResultSet.Next()
		tblName, err := row.GetString(0)
		c.Assert(err, IsNil)
		if tblName == sqlTableNameWithSearch {
			hasTable = true
		}
	}
	c.Assert(hasTable, Equals, true)

	log.Println("sql query via FLAT_BUFFERS")
	resp, err = client.SQLQuery(&SQLQueryRequest{Query: fmt.Sprintf("select * from %s", sqlTableNameWithSearch)})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_SELECT)
	c.Assert(resp.SQLQueryConsumed, NotNil)
	cbytes, _ := json.Marshal(resp.SQLQueryConsumed)
	log.Println(string(cbytes))
	c.Assert(len(resp.SQLQueryConsumed.SearchConsumes), Equals, 1)
	c.Assert(len(resp.SQLQueryConsumed.TableConsumes), Equals, 0)
	c.Assert(resp.SQLQueryConsumed.SearchConsumes[0].TableName, Equals, sqlTableNameWithSearch)
	c.Assert(resp.SQLQueryConsumed.SearchConsumes[0].IndexName, Equals, sqlSearchName)
	c.Assert(resp.SQLQueryConsumed.SearchConsumes[0].ConsumedCapacityUnit.Read > 0, Equals, true)
	resultSet := resp.ResultSet
	c.Assert(len(resultSet.Columns()), Equals, 4)
	i := 0
	for resultSet.HasNext() {
		sqlRow := resultSet.Next()
		log.Println(sqlRow.DebugString())

		val, err := sqlRow.GetInt64(0)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, int64(i))
		val, err = sqlRow.GetInt64ByName("a")
		c.Assert(err, IsNil)
		c.Assert(val, Equals, int64(i))

		val2, err := sqlRow.GetFloat64(1)
		c.Assert(err, IsNil)
		c.Assert(val2, Equals, float64(i))
		val2, err = sqlRow.GetFloat64ByName("b")
		c.Assert(err, IsNil)
		c.Assert(val2, Equals, float64(i))

		if i == 3 {
			val, err := sqlRow.IsNull(2)
			c.Assert(err, IsNil)
			c.Assert(val, Equals, true)
		} else {
			val, err := sqlRow.GetString(2)
			c.Assert(err, IsNil)
			c.Assert(val, Equals, strconv.Itoa(i))
			val, err = sqlRow.GetStringByName("c")
			c.Assert(err, IsNil)
			c.Assert(val, Equals, strconv.Itoa(i))
		}

		val4, err := sqlRow.GetBool(3)
		c.Assert(err, IsNil)
		c.Assert(val4, Equals, i%2 == 1)
		i++
	}
}

func (s *TableStoreSuite) TestSQLTimeSeries(c *C) {
	// devops_25w is timeseries table, prepared in advanced.
	instanceName := os.Getenv("OTS_TEST_INSTANCENAME")
	if !strings.Contains(instanceName, "test-sql-e2e") {
		c.Skip("devops_25w is timeseries table, should prepared in advanced.")
	}
	resp, err := client.SQLQuery(&SQLQueryRequest{Query: "select * from devops_25w limit 10"})
	c.Assert(err, IsNil)
	c.Assert(resp.StmtType, Equals, SQL_SELECT)
	c.Assert(resp.SQLQueryConsumed, NotNil)
	c.Assert(len(resp.SQLQueryConsumed.SearchConsumes), Equals, 0)
	c.Assert(len(resp.SQLQueryConsumed.TableConsumes), Equals, 1)
	c.Assert(resp.SQLQueryConsumed.TableConsumes[0].TableName, Equals, "devops_25w")
	c.Assert(resp.SQLQueryConsumed.TableConsumes[0].ConsumedCapacityUnit.Read > 0, Equals, true)
	resultSet := resp.ResultSet
	c.Assert(len(resultSet.Columns()), Equals, 12)
	for resultSet.HasNext() {
		sqlRow := resultSet.Next()
		log.Println(sqlRow.DebugString())
		isnull, err := sqlRow.IsNullByName("_m_name")
		c.Assert(err, IsNil)
		c.Assert(isnull, Equals, false)
		_, err = sqlRow.GetBytesByName("_m_name")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not BINARY")
		_, err = sqlRow.GetInt64ByName("_m_name")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not INTEGER")
		name, err := sqlRow.GetStringByName("_m_name")
		c.Assert(name, Equals, "kernel")
		c.Assert(err, IsNil)
		_, err = sqlRow.GetBoolByName("_m_name")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not BOOLEAN")
		_, err = sqlRow.GetFloat64ByName("_m_name")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "the type of column is not DOUBLE")

		// test nil
		isnull, err = sqlRow.IsNullByName("_string_value")
		c.Assert(err, IsNil)
		c.Assert(isnull, Equals, true)
	}
}
