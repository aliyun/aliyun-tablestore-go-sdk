package tablestore

import (
	"testing"
	. "gopkg.in/check.v1"
	"fmt"
	"os"
	"strconv"
	"strings"
	"runtime"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

type TsSuite struct{}

var tableNamePrefix string

var _ = Suite(&TsSuite{})

var defaultTableName = "defaulttable"

// Todo: use config
var client OtsApi

func (s *TsSuite) SetUpSuite(c *C) {

	endpoint := os.Getenv("TS_TEST_ENDPOINT")
	instanceName := os.Getenv("TS_TEST_INSTANCENAME")
	accessKeyId := os.Getenv("TS_TEST_KEYID")
	accessKeySecret := os.Getenv("TS_TEST_SECRET")
	client = NewClient(endpoint, instanceName, accessKeyId, accessKeySecret)

	tableNamePrefix = strings.Replace(runtime.Version(), ".", "", -1)
	defaultTableName = tableNamePrefix + defaultTableName
	PrepareTable(defaultTableName)
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

func (s *TsSuite) TestCreateTable(c *C) {
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
}

func (s *TsSuite) TestListTable(c *C) {
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

func (s *TsSuite) TestUpdateAndDescribeTable(c *C) {
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

func (s *TsSuite) TestTableWithKeyAutoIncrement(c *C) {
	tableName := "incrementtable"
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
		_, error := client.PutRow(putRowRequest)
		c.Check(error, Equals, nil)
	}
}

func (s *TsSuite) TestPutGetRow(c *C) {
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
	c.Check(len(getResp.Columns), Equals, 6)
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
	fmt.Println("TestPutGetRow finished")
}

func (s *TsSuite) TestPutGetRowWithFilter(c *C) {
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
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	clCondition1 := NewSingleColumnCondition("col2", CT_EQUAL, int64(100))
	clCondition2 := NewSingleColumnCondition("col5", CT_EQUAL, int64(50))
	clCondition3 := NewSingleColumnCondition("col6", CT_LESS_THAN, int64(100))
	cf := NewCompositeColumnCondition(LO_AND)
	cf.AddFilter(clCondition1)
	cf.AddFilter(clCondition2)
	cf.AddFilter(clCondition3)
	putRowChange.SetColumnCondition(cf)

	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	c.Check(error, Equals, nil)

	getRowRequest := new(GetRowRequest)
	criteria := new(SingleRowQueryCriteria);
	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = defaultTableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	getRowRequest.SingleRowQueryCriteria.SetFilter(cf)
	getResp, error := client.GetRow(getRowRequest)
	c.Check(error, Equals, nil)
	c.Check(getResp, NotNil)
	c.Check(len(getResp.PrimaryKey.PrimaryKeys), Equals, 1)
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].ColumnName, Equals, "pk1")
	c.Check(getResp.PrimaryKey.PrimaryKeys[0].Value, Equals, "Key6")
	c.Check(len(getResp.Columns), Equals, 6)
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
	fmt.Println("TestPutGetRowWithFilter finished")
}

func (s *TsSuite) TestPutUpdateDeleteRow(c *C) {
	fmt.Println("TestPutUpdateDeleteRow started")
	keyToUpdate := "pk1toupdate"
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", keyToUpdate)
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", "col1data1")
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
	fmt.Println("TestPutUpdateDeleteRow finished")
}

func (s *TsSuite) TestBatchGetRow(c *C) {
	fmt.Println("TestBatchGetRow started")
	rowCount := 100
	for i := 0; i < rowCount; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		PrepareDataInDefaultTable(key, value)
	}

	batchGetReq := &BatchGetRowRequest{}
	mqCriteria := &MultiRowQueryCriteria{}

	for i := 0; i < rowCount; i++ {
		pkToGet := new(PrimaryKey)
		key := "key" + strconv.Itoa(i)
		pkToGet.AddPrimaryKeyColumn("pk1", key)
		mqCriteria.AddRow(pkToGet)
		mqCriteria.MaxVersion = 1
	}

	mqCriteria.TableName = defaultTableName
	batchGetReq.MultiRowQueryCriteria = append(batchGetReq.MultiRowQueryCriteria, mqCriteria)
	batchGetResponse, error := client.BatchGetRow(batchGetReq)
	c.Check(error, Equals, nil)

	c.Check(len(batchGetResponse.TableToRowsResult), Equals, 1)
	c.Check(len(batchGetResponse.TableToRowsResult[mqCriteria.TableName]), Equals, rowCount)

	for _, rowToCheck := range (batchGetResponse.TableToRowsResult[mqCriteria.TableName]) {
		c.Check(rowToCheck.TableName, Equals, mqCriteria.TableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
		c.Check(len(rowToCheck.PrimaryKey.PrimaryKeys), Equals, 1)
		c.Check(len(rowToCheck.Columns), Equals, 1)
	}

	fmt.Println("TestBatchGetRow started")
}

func (s *TsSuite) TestBatchWriteRow(c *C) {
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
	for _, rowToCheck := range (batchWriteResponse.TableToRowsResult[defaultTableName]) {
		c.Check(rowToCheck.TableName, Equals, defaultTableName)
		c.Check(rowToCheck.IsSucceed, Equals, true)
	}
	fmt.Println("TestBatchWriteRow finished")
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

func PrepareDataInDefaultTable(key string, value string) error {
	putRowRequest := new(PutRowRequest)
	putRowChange := new(PutRowChange)
	putRowChange.TableName = defaultTableName
	putPk := new(PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", key)
	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("col1", value)
	putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, error := client.PutRow(putRowRequest)
	return error
}