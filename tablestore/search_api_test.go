package tablestore

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/golang/protobuf/proto"
	. "gopkg.in/check.v1"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

//run with "gocheck"

type SearchSuite struct{}

var _ = Suite(&SearchSuite{})

//for aggregation
var searchAPITestTableName1 = "search_api_test_table1"
var searchAPITestIndexName1 = "search_api_test_index1"

//for group by
var searchAPITestTableName2 = "search_api_test_table2"
var searchAPITestIndexName2 = "search_api_test_index2"

func DeleteTable(c *C, client TableStoreApi, tableName string) {
	req := new(DeleteTableRequest)
	req.TableName = tableName
	resp, err := client.DeleteTable(req)
	c.Log("DeleteTable", resp.RequestId)
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		return
	}
	c.Check(err, IsNil)
	c.Log("delete", tableName)
}

func ListTable(c *C, client TableStoreApi) []string {
	resp, err := client.ListTable()
	c.Log("ListTable", resp.RequestId)
	c.Check(err, IsNil)
	return resp.TableNames
}

func DeleteIndex(c *C, client TableStoreApi, tableName string, indexName string) {
	req := new(DeleteSearchIndexRequest)
	req.TableName = tableName
	req.IndexName = indexName
	resp, err := client.DeleteSearchIndex(req)
	c.Log("DeleteIndex", resp.ResponseInfo.RequestId)
	if err != nil && strings.Contains(err.Error(), "OTSObjectNotExist") {
		return
	}
	c.Check(err, IsNil)
	c.Log("delete table:", tableName, "index:", indexName)
}

func DescribeSearchIndex(c *C, client TableStoreApi, tableName string, indexName string) *DescribeSearchIndexResponse {
	req := new(DescribeSearchIndexRequest)
	req.TableName = tableName
	req.IndexName = indexName
	resp, err := client.DescribeSearchIndex(req)
	c.Log("DescribeSearchIndex", resp.ResponseInfo.RequestId)
	c.Check(err, IsNil)
	return resp
}

func in(target string, strArray []string) bool {
	sort.Strings(strArray)
	index := sort.SearchStrings(strArray, target)
	if index < len(strArray) && strArray[index] == target {
		return true
	}
	return false
}

func DeleteTableAndAllIndex(c *C, client TableStoreApi, tableName string) {
	if !in(tableName, ListTable(c, client)) {
		return
	}
	indices := ListSearchIndex(c, client, tableName)
	for _, index := range indices {
		DeleteIndex(c, client, tableName, index)
	}
	DeleteTable(c, client, tableName)
}

func getNormalTestIndexSchemaWithNested() *IndexSchema {
	return &IndexSchema{
		FieldSchemas: []*FieldSchema{
			{
				FieldName:   proto.String("date"),
				FieldType:   FieldType_DATE,
				DateFormats: []string{"yyyy-MM-dd'T'HH:mm:ss.SSSSSS", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS"},
			},
			{
				FieldName: proto.String("nested"),
				FieldType: FieldType_NESTED,
				FieldSchemas: []*FieldSchema{
					{
						FieldName:   proto.String("nested_date"),
						FieldType:   FieldType_DATE,
						DateFormats: []string{"epoch_micros"},
					},
				},
			},
		},
	}
}

func getNormalTestIndexSchema() *IndexSchema {
	return &IndexSchema{
		FieldSchemas: []*FieldSchema{
			{
				FieldName: proto.String("pk1"),
				FieldType: FieldType_KEYWORD,
			},
			{
				FieldName: proto.String("col1"),
				FieldType: FieldType_KEYWORD,
			},
			{
				FieldName: proto.String("col2"),
				FieldType: FieldType_TEXT,
				EnableHighlighting: proto.Bool(true),
			},
		},
	}
}

func CreateSearchTable(c *C, client TableStoreApi, tableName string) {
	createTableRequest := new(CreateTableRequest)
	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput
	resp, err := client.CreateTable(createTableRequest)
	c.Log("CreateSearchTable", resp.RequestId)
	c.Check(err, IsNil)
}

func CreateSearchTableAndDisallowUpdate(c *C, client TableStoreApi, tableName string) {
	createTableRequest := new(CreateTableRequest)
	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	allowUpdate := false
	tableOption.AllowUpdate = &allowUpdate
	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput
	resp, err := client.CreateTable(createTableRequest)
	c.Log("CreateSearchTable", resp.RequestId)
	c.Check(err, IsNil)
}

func ListSearchIndex(c *C, client TableStoreApi, tableName string) []string {
	req := new(ListSearchIndexRequest)
	req.TableName = tableName
	resp, err := client.ListSearchIndex(req)
	c.Log("ListSearchIndex", resp.ResponseInfo.RequestId)
	c.Check(err, IsNil)
	var indices []string
	for _, indexInfo := range resp.IndexInfo {
		indices = append(indices, indexInfo.IndexName)
	}
	return indices
}

func CreateSearchIndex(c *C, client TableStoreApi, tableName string, indexName string, sourceIndexName *string, indexSchema *IndexSchema, ttl int32) {
	req := new(CreateSearchIndexRequest)
	req.TableName = tableName
	req.IndexName = indexName
	req.SourceIndexName = sourceIndexName
	req.IndexSchema = indexSchema
	req.TimeToLive = &ttl
	resp, err := client.CreateSearchIndex(req)
	if err != nil {
		c.Log(fmt.Println(err))
	}
	c.Log("CreateSearchIndex", resp.ResponseInfo.RequestId)
	c.Check(err, IsNil)
}

func WaitDataSyncByMatchAllQuery(c *C, client TableStoreApi, expectCount int64, tableName string, indexName string, timeInSecond int) {
	searchRequest := new(SearchRequest)
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	searchQuery := search.NewSearchQuery()
	searchQuery.Limit = 0
	searchQuery.SetQuery(&search.MatchAllQuery{})
	searchQuery.SetGetTotalCount(true)
	searchRequest.SetSearchQuery(searchQuery)
	errTimes := 0
	for i := 1; i <= timeInSecond; i++ {
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			errTimes++
			if errTimes > 10 {
				c.Fatal(time.Now().Local(), i, "times query", "MatchAllQuery failed!", err)
				return
			}
			c.Log(i, "times query", "MatchAllQuery failed!", err)
		} else {
			if i == timeInSecond {
				c.Log(i, "times query", "MatchAllQuery Hits: ", searchResponse.TotalCount, "  RequestId:", searchResponse.RequestId)
			}
			if expectCount == searchResponse.TotalCount {
				c.Log(i, "times query.", "MatchAllQuery Hits: ", searchResponse.TotalCount, "  RequestId:", searchResponse.RequestId)
				time.Sleep(time.Duration(2) * time.Second)
				return
			}
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	c.Fatal("after ", timeInSecond, "seconds, sync date filed", time.Now().Local())
}

func createTable1(c *C) {
	fmt.Println("Begin to create table:", searchAPITestTableName1)
	createTableRequest := new(CreateTableRequest)

	tableMeta := new(TableMeta)
	tableMeta.TableName = searchAPITestTableName1
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput

	_, err := client.CreateTable(createTableRequest)
	if err != nil {
		c.Fatal("Failed to create table with error: ", err)
	} else {
		fmt.Println("Create table finished")
	}
}

func createTable2(c *C) {
	fmt.Println("Begin to create table:", searchAPITestTableName2)
	createTableRequest := new(CreateTableRequest)

	tableMeta := new(TableMeta)
	tableMeta.TableName = searchAPITestTableName2
	tableMeta.AddPrimaryKeyColumn("pk1", PrimaryKeyType_STRING)
	tableOption := new(TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput

	_, err := client.CreateTable(createTableRequest)
	if err != nil {
		c.Fatal("Failed to create table with error: ", err)
	} else {
		fmt.Println("Create table finished")
	}
}

func deleteTable(tableName string) {
	deleteRequest := new(DeleteTableRequest)
	deleteRequest.TableName = tableName
	client.DeleteTable(deleteRequest)
}

func createSearchIndex1(c *C) {
	fmt.Println("Begin to create index:", searchAPITestIndexName1)
	request := &CreateSearchIndexRequest{}
	request.TableName = searchAPITestTableName1
	request.IndexName = searchAPITestIndexName1

	var schemas []*FieldSchema
	field1 := &FieldSchema{
		FieldName:        proto.String("Col_Long"),
		FieldType:        FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field2 := &FieldSchema{
		FieldName:        proto.String("Col_Double"),
		FieldType:        FieldType_DOUBLE,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field3 := &FieldSchema{
		FieldName:        proto.String("Col_Boolean"),
		FieldType:        FieldType_BOOLEAN,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field4 := &FieldSchema{
		FieldName:        proto.String("Col_Keyword"),
		FieldType:        FieldType_KEYWORD,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field5 := &FieldSchema{
		FieldName:        proto.String("Col_GeoPoint"),
		FieldType:        FieldType_GEO_POINT,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field6 := &FieldSchema{
		FieldName: proto.String("Col_Text"),
		FieldType: FieldType_TEXT,
		Index:     proto.Bool(true),
		EnableHighlighting: proto.Bool(true),
	}
	field7 := &FieldSchema{
		FieldName: proto.String("Col_Nested"),
		FieldType: FieldType_NESTED,
		FieldSchemas: []*FieldSchema{
			{
				FieldName:        proto.String("Col_Long_Nested"),
				FieldType:        FieldType_LONG,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Double_Nested"),
				FieldType:        FieldType_DOUBLE,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Boolean_Nested"),
				FieldType:        FieldType_BOOLEAN,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Keyword_Nested"),
				FieldType:        FieldType_KEYWORD,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_GeoPoint_Nested"),
				FieldType:        FieldType_GEO_POINT,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName: proto.String("Col_Text_Nested"),
				FieldType: FieldType_TEXT,
				Index:     proto.Bool(true),
			},
		},
	}
	schemas = append(schemas, field1, field2, field3, field4, field5, field6, field7)

	//fields with missing value
	field11 := &FieldSchema{
		FieldName:        proto.String("Col_Long_Missing"),
		FieldType:        FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field12 := &FieldSchema{
		FieldName:        proto.String("Col_Double_Missing"),
		FieldType:        FieldType_DOUBLE,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field13 := &FieldSchema{
		FieldName:        proto.String("Col_Boolean_Missing"),
		FieldType:        FieldType_BOOLEAN,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field14 := &FieldSchema{
		FieldName:        proto.String("Col_Keyword_Missing"),
		FieldType:        FieldType_KEYWORD,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field15 := &FieldSchema{
		FieldName:        proto.String("Col_GeoPoint_Missing"),
		FieldType:        FieldType_GEO_POINT,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field16 := &FieldSchema{
		FieldName: proto.String("Col_Text_Missing"),
		FieldType: FieldType_TEXT,
		Index:     proto.Bool(true),
	}
	field17 := &FieldSchema{
		FieldName: proto.String("Col_Nested_Missing"),
		FieldType: FieldType_NESTED,
		FieldSchemas: []*FieldSchema{
			{
				FieldName:        proto.String("Col_Long_Missing_Nested"),
				FieldType:        FieldType_LONG,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Double_Missing_Nested"),
				FieldType:        FieldType_DOUBLE,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Boolean_Missing_Nested"),
				FieldType:        FieldType_BOOLEAN,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Keyword_Missing_Nested"),
				FieldType:        FieldType_KEYWORD,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_GeoPoint_Missing_Nested"),
				FieldType:        FieldType_GEO_POINT,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName: proto.String("Col_Text_Missing_Nested"),
				FieldType: FieldType_TEXT,
				Index:     proto.Bool(true),
			},
		},
	}
	schemas = append(schemas, field11, field12, field13, field14, field15, field16, field17)
	
	// highlight column
	field18 := &FieldSchema{
		FieldName: proto.String("Col_Highlight_Text"),
		FieldType: FieldType_TEXT,
		Index:     proto.Bool(true),
		EnableHighlighting: proto.Bool(true),
	}
	schemas = append(schemas, field18)

	request.IndexSchema = &IndexSchema{
		FieldSchemas: schemas,
	}
	_, err := client.CreateSearchIndex(request)
	if err != nil {
		c.Fatal("Failed to create search index with error: ", err)
	} else {
		fmt.Println("Create search index finished")
	}
}

func createSearchIndex2(c *C) {
	fmt.Println("Begin to create index:", searchAPITestIndexName2)
	request := &CreateSearchIndexRequest{}
	request.TableName = searchAPITestTableName2
	request.IndexName = searchAPITestIndexName2

	var schemas []*FieldSchema
	field1 := &FieldSchema{
		FieldName:        proto.String("Col_Long"),
		FieldType:        FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field2 := &FieldSchema{
		FieldName:        proto.String("Col_Double"),
		FieldType:        FieldType_DOUBLE,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field3 := &FieldSchema{
		FieldName:        proto.String("Col_Boolean"),
		FieldType:        FieldType_BOOLEAN,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field4 := &FieldSchema{
		FieldName:        proto.String("Col_Keyword"),
		FieldType:        FieldType_KEYWORD,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field5 := &FieldSchema{
		FieldName:        proto.String("Col_GeoPoint"),
		FieldType:        FieldType_GEO_POINT,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field6 := &FieldSchema{
		FieldName: proto.String("Col_Text"),
		FieldType: FieldType_TEXT,
		Index:     proto.Bool(true),
	}
	field7 := &FieldSchema{
		FieldName: proto.String("Col_Nested"),
		FieldType: FieldType_NESTED,
		FieldSchemas: []*FieldSchema{
			{
				FieldName:        proto.String("Col_Long_Nested"),
				FieldType:        FieldType_LONG,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Double_Nested"),
				FieldType:        FieldType_DOUBLE,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Boolean_Nested"),
				FieldType:        FieldType_BOOLEAN,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_Keyword_Nested"),
				FieldType:        FieldType_KEYWORD,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName:        proto.String("Col_GeoPoint_Nested"),
				FieldType:        FieldType_GEO_POINT,
				Index:            proto.Bool(true),
				EnableSortAndAgg: proto.Bool(true),
			},
			{
				FieldName: proto.String("Col_Text_Nested"),
				FieldType: FieldType_TEXT,
				Index:     proto.Bool(true),
			},
		},
	}
	schemas = append(schemas, field1, field2, field3, field4, field5, field6, field7)

	request.IndexSchema = &IndexSchema{
		FieldSchemas: schemas,
	}
	_, err := client.CreateSearchIndex(request)
	if err != nil {
		c.Fatal("Failed to create search index with error: ", err)
	} else {
		fmt.Println("Create search index finished")
	}
}

func deleteSearchIndex(tableName string, indexName string) {
	deleteIndexRequest := new(DeleteSearchIndexRequest)
	deleteIndexRequest.TableName = tableName
	deleteIndexRequest.IndexName = indexName
	client.DeleteSearchIndex(deleteIndexRequest)
}

func writeData1(c *C) {
	strs := []string{"hangzhou", "tablestore", "ots"}
	highlightText := []string{"dengcai <em>street</em>, xihu district, hangzhou city"}
	geopoints := []string{
		"30.137817,120.08681",  //飞天园区
		"30.135131,120.088355", //中大银座
		"30.181877,120.152818", //中医药地铁站
		"30.20223,120.13787",   //六和塔
		"30.216961,120.157633", //八卦田
		"30.231566,120.148578", //太子湾
		"30.26058,120.170712",  //龙翔桥
		"30.269501,120.169347", //凤起路
		"30.28073,120.168843",  //运河
		"30.296946,120.21958",  //杭州东站
	}

	for i := 0; i < 10; i++ { //0, 1, ..., 9
		putRowRequest := new(PutRowRequest)
		putRowChange := new(PutRowChange)
		putRowChange.TableName = searchAPITestTableName1
		putPk := new(PrimaryKey)
		putPk.AddPrimaryKeyColumn("pk1", fmt.Sprintf("pk_%d", i))

		longValue := int64(i)
		doubleValue := float64(i) + 0.1
		boolValue := false
		if i%2 == 0 {
			boolValue = true
		}
		keywordValue := strs[i%len(strs)]
		geoPointValue := geopoints[i]
		textValue := strs[i%len(strs)]
		highlightTextValue := highlightText[i % len(highlightText)]
		nestedValue := fmt.Sprintf("[{\"Col_Long_Nested\": %v, \"Col_Double_Nested\": %v, \"Col_Boolean_Nested\": %v, \"Col_Keyword_Nested\": \"%v\", \"Col_GeoPoint_Nested\": \"%v\", \"Col_Text_Nested\": \"%v\"}]",
			longValue, doubleValue, boolValue, keywordValue, geoPointValue, textValue)
		nestedMissingValue := fmt.Sprintf("[{\"Col_Long_Missing_Nested\": %v, \"Col_Double_Missing_Nested\": %v, \"Col_Boolean_Missing_Nested\": %v, \"Col_Keyword_Missing_Nested\": \"%v\", \"Col_GeoPoint_Missing_Nested\": \"%v\", \"Col_Text_Missing_Nested\": \"%v\"}]",
			longValue, doubleValue, boolValue, keywordValue, geoPointValue, textValue)

		putRowChange.PrimaryKey = putPk
		putRowChange.AddColumn("Col_Long", longValue)
		putRowChange.AddColumn("Col_Double", doubleValue)
		putRowChange.AddColumn("Col_Boolean", boolValue)
		putRowChange.AddColumn("Col_Keyword", keywordValue)
		putRowChange.AddColumn("Col_GeoPoint", geoPointValue)
		putRowChange.AddColumn("Col_Text", textValue)
		putRowChange.AddColumn("Col_Nested", nestedValue)
		putRowChange.AddColumn("Col_Highlight_Text", highlightTextValue)

		if i >= 5 { //leave out the first 5 rows
			putRowChange.AddColumn("Col_Long_Missing", longValue)
			putRowChange.AddColumn("Col_Double_Missing", doubleValue)
			putRowChange.AddColumn("Col_Boolean_Missing", boolValue)
			putRowChange.AddColumn("Col_Keyword_Missing", keywordValue)
			putRowChange.AddColumn("Col_GeoPoint_Missing", geoPointValue)
			putRowChange.AddColumn("Col_Text_Missing", textValue)
			putRowChange.AddColumn("Col_Nested_Missing", nestedMissingValue)
		}

		putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
		putRowRequest.PutRowChange = putRowChange
		if _, err := client.PutRow(putRowRequest); err != nil {
			c.Fatal("putRow failed with error: ", err)
		}
	}
}

func writeData2(c *C) {
	longs := []int64{1, 2, 2, 3, 3, 3, 4, 4, 4, 4}
	doubles := []float64{1.1, 2.1, 2.1, 3.1, 3.1, 3.1, 4.1, 4.1, 4.1, 4.1}
	bools := []bool{false, false, false, false, true, true, true, true, true, true}
	strs := []string{"hangzhou", "hangzhou", "hangzhou", "hangzhou", "tablestore", "tablestore", "tablestore", "tablestore", "tablestore", "tablestore"}
	geopoints := []string{
		"30.137817,120.08681",  //飞天园区
		"30.135131,120.088355", //中大银座
		"30.181877,120.152818", //中医药地铁站
		"30.20223,120.13787",   //六和塔
		"30.216961,120.157633", //八卦田
		"30.231566,120.148578", //太子湾
		"30.26058,120.170712",  //龙翔桥
		"30.269501,120.169347", //凤起路
		"30.28073,120.168843",  //运河
		"30.296946,120.21958",  //杭州东站
	}

	for i := 0; i < 10; i++ { //0, 1, ..., 9
		putRowRequest := new(PutRowRequest)
		putRowChange := new(PutRowChange)
		putRowChange.TableName = searchAPITestTableName2
		putPk := new(PrimaryKey)
		putPk.AddPrimaryKeyColumn("pk1", fmt.Sprintf("pk_%d", i))

		longValue := longs[i]
		doubleValue := doubles[i]
		boolValue := bools[i]
		keywordValue := strs[i]
		geoPointValue := geopoints[i]
		textValue := strs[i]
		nestedValue := fmt.Sprintf("[{\"Col_Long_Nested\": %v, \"Col_Double_Nested\": %v, \"Col_Boolean_Nested\": %v, \"Col_Keyword_Nested\": \"%v\", \"Col_GeoPoint_Nested\": \"%v\", \"Col_Text_Nested\": \"%v\"}]",
			longValue, doubleValue, boolValue, keywordValue, geoPointValue, textValue)
		nestedMissingValue := fmt.Sprintf("[{\"Col_Long_Missing_Nested\": %v, \"Col_Double_Missing_Nested\": %v, \"Col_Boolean_Missing_Nested\": %v, \"Col_Keyword_Missing_Nested\": \"%v\", \"Col_GeoPoint_Missing_Nested\": \"%v\", \"Col_Text_Missing_Nested\": \"%v\"}]",
			longValue, doubleValue, boolValue, keywordValue, geoPointValue, textValue)

		putRowChange.PrimaryKey = putPk
		putRowChange.AddColumn("Col_Long", longValue)
		putRowChange.AddColumn("Col_Double", doubleValue)
		putRowChange.AddColumn("Col_Boolean", boolValue)
		putRowChange.AddColumn("Col_Keyword", keywordValue)
		putRowChange.AddColumn("Col_GeoPoint", geoPointValue)
		putRowChange.AddColumn("Col_Text", textValue)
		putRowChange.AddColumn("Col_Nested", nestedValue)

		if i >= 5 { //leave out the first 5 rows
			putRowChange.AddColumn("Col_Long_Missing", longValue)
			putRowChange.AddColumn("Col_Double_Missing", doubleValue)
			putRowChange.AddColumn("Col_Boolean_Missing", boolValue)
			putRowChange.AddColumn("Col_Keyword_Missing", keywordValue)
			putRowChange.AddColumn("Col_GeoPoint_Missing", geoPointValue)
			putRowChange.AddColumn("Col_Text_Missing", textValue)
			putRowChange.AddColumn("Col_Nested_Missing", nestedMissingValue)
		}

		putRowChange.SetCondition(RowExistenceExpectation_IGNORE)
		putRowRequest.PutRowChange = putRowChange
		if _, err := client.PutRow(putRowRequest); err != nil {
			c.Fatal("putRow failed with error: ", err)
		}
	}
}

func (s *SearchSuite) SetUpSuite(c *C) {
	endpoint := os.Getenv("OTS_TEST_ENDPOINT")
	instanceName := os.Getenv("OTS_TEST_INSTANCENAME")
	accessKeyId := os.Getenv("OTS_TEST_KEYID")
	accessKeySecret := os.Getenv("OTS_TEST_SECRET")

	client = NewClient(endpoint, instanceName, accessKeyId, accessKeySecret)

	//clear old environment
	deleteSearchIndex(searchAPITestTableName1, searchAPITestIndexName1)
	deleteTable(searchAPITestTableName1)

	deleteSearchIndex(searchAPITestTableName2, searchAPITestIndexName2)
	deleteTable(searchAPITestTableName2)

	//init new environment
	createTable1(c)
	createSearchIndex1(c)

	createTable2(c)
	createSearchIndex2(c)

	writeData1(c)
	writeData2(c)

	WaitDataSyncByMatchAllQuery(c, client, 10, searchAPITestTableName1, searchAPITestIndexName1, 40)
	WaitDataSyncByMatchAllQuery(c, client, 10, searchAPITestTableName2, searchAPITestIndexName2, 40)
}

/* avg agg */

func (s *SearchSuite) TestAggregationAvgAggregationEmptyAggName(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationAvgAggregationValidType(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Long")).
			Aggregation(search.NewAvgAggregation("agg2", "Col_Double"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	{
		aggResult, err := searchResponse.AggregationResults.Avg("agg1")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, 4.5)
	}
	{
		aggResult, err := searchResponse.AggregationResults.Avg("agg2")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, 4.6)
	}
}

func (s *SearchSuite) TestAggregationAvgAggregationValidTypeMissingValue(c *C) {
	//no missing value provided
	{
		searchRequest := &SearchRequest{}
		searchRequest.
			SetTableName(searchAPITestTableName1).
			SetIndexName(searchAPITestIndexName1).
			SetSearchQuery(search.NewSearchQuery().
				SetQuery(&search.MatchAllQuery{}).
				SetLimit(100).
				Aggregation(search.NewAvgAggregation("agg1", "Col_Long_Missing")).
				Aggregation(search.NewAvgAggregation("agg2", "Col_Double_Missing"))).
			SetColumnsToGet(&ColumnsToGet{
				ReturnAll: false,
			})
		searchResponse, err := client.Search(searchRequest)
		c.Check(err, Equals, nil)
		{
			aggResult, err := searchResponse.AggregationResults.Avg("agg1")
			c.Check(err, Equals, nil)
			c.Check(aggResult.Value, Equals, 7.0)
		}
		{
			aggResult, err := searchResponse.AggregationResults.Avg("agg2")
			c.Check(err, Equals, nil)
			c.Check(aggResult.Value, Equals, 7.1)
		}
	}

	//missing value provided
	{
		searchRequest := &SearchRequest{}
		searchRequest.
			SetTableName(searchAPITestTableName1).
			SetIndexName(searchAPITestIndexName1).
			SetSearchQuery(search.NewSearchQuery().
				SetQuery(&search.MatchAllQuery{}).
				SetLimit(100).
				Aggregation(search.NewAvgAggregation("agg1", "Col_Long_Missing").Missing(9)).
				Aggregation(search.NewAvgAggregation("agg2", "Col_Double_Missing").Missing(9.1))).
			SetColumnsToGet(&ColumnsToGet{
				ReturnAll: false,
			})
		searchResponse, err := client.Search(searchRequest)
		c.Check(err, Equals, nil)
		{
			aggResult, err := searchResponse.AggregationResults.Avg("agg1")
			c.Check(err, Equals, nil)
			c.Check(aggResult.Value, Equals, 8.0)
		}
		{
			aggResult, err := searchResponse.AggregationResults.Avg("agg2")
			c.Check(err, Equals, nil)
			c.Check(aggResult.Value, Equals, 8.1)
		}
	}
}

func (s *SearchSuite) TestAggregationAvgAggregationInvalidTypeBoolean(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Boolean"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationAvgAggregationInvalidTypeKeyword(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Keyword"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationAvgAggregationInvalidTypeGeoPoint(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_GeoPoint"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationAvgAggregationInvalidTypeNested(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Nested"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationAvgAggregationInvalidTypeText(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Text"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationAvgAggregationUnknownField(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Unknown"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[avg agg\\] field: Col_Unknown does not exist.*")
}

/* max agg */

func (s *SearchSuite) TestAggregationMaxAggregationEmptyAggName(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMaxAggregation("", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMaxAggregationValidType(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMaxAggregation("agg1", "Col_Long")).
			Aggregation(search.NewMaxAggregation("agg2", "Col_Double"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	{
		aggResult, err := searchResponse.AggregationResults.Max("agg1")
		c.Check(err, Equals, nil)
		c.Check(int64(aggResult.Value), Equals, int64(9))
	}
	{
		aggResult, err := searchResponse.AggregationResults.Max("agg2")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, 9.1)
	}
}

func (s *SearchSuite) TestAggregationMaxAggregationInvalidTypeBoolean(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMaxAggregation("agg1", "Col_Boolean"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMaxAggregationInvalidTypeKeyword(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMaxAggregation("agg1", "Col_Keyword"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMaxAggregationInvalidTypeGeoPoint(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMaxAggregation("agg1", "Col_GeoPoint"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMaxAggregationInvalidTypeNested(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMaxAggregation("agg1", "Col_Nested"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMaxAggregationUnknownField(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMaxAggregation("agg1", "Col_Unknown"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[max agg\\] field: Col_Unknown does not exist.*")
}

/* min agg */

func (s *SearchSuite) TestAggregationMinAggregationEmptyAggName(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMinAggregation("", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMinAggregationValidType(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMinAggregation("agg1", "Col_Long")).
			Aggregation(search.NewMinAggregation("agg2", "Col_Double"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	{
		aggResult, err := searchResponse.AggregationResults.Min("agg1")
		c.Check(err, Equals, nil)
		c.Check(int64(aggResult.Value), Equals, int64(0))
	}
	{
		aggResult, err := searchResponse.AggregationResults.Min("agg2")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, 0.1)
	}
}

func (s *SearchSuite) TestAggregationMinAggregationInvalidTypeBoolean(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMinAggregation("agg1", "Col_Boolean"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMinAggregationInvalidTypeKeyword(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMinAggregation("agg1", "Col_Keyword"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMinAggregationInvalidTypeGeoPoint(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMinAggregation("agg1", "Col_GeoPoint"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMinAggregationInvalidTypeNested(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMinAggregation("agg1", "Col_Nested"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationMinAggregationUnknownField(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewMinAggregation("agg1", "Col_Unknown"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[min agg\\] field: Col_Unknown does not exist.*")
}

/* sum agg */

func (s *SearchSuite) TestAggregationSumAggregationEmptyAggName(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewSumAggregation("", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationSumAggregationValidType(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewSumAggregation("agg1", "Col_Long")).
			Aggregation(search.NewSumAggregation("agg2", "Col_Double"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	{
		aggResult, err := searchResponse.AggregationResults.Sum("agg1")
		c.Check(err, Equals, nil)
		c.Check(int64(aggResult.Value), Equals, int64(45))
	}
	{
		aggResult, err := searchResponse.AggregationResults.Sum("agg2")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, 46.0)
	}
}

func (s *SearchSuite) TestAggregationSumAggregationInvalidTypeBoolean(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewSumAggregation("agg1", "Col_Boolean"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[sum agg\\] field_name:Col_Boolean type:boolean is invalid, allow \\[long, double\\].*")
}

func (s *SearchSuite) TestAggregationSumAggregationInvalidTypeKeyword(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewSumAggregation("agg1", "Col_Keyword"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[sum agg\\] field_name:Col_Keyword type:keyword is invalid, allow \\[long, double\\].*")
}

func (s *SearchSuite) TestAggregationSumAggregationInvalidTypeGeoPoint(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewSumAggregation("agg1", "Col_GeoPoint"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[sum agg\\] field_name:Col_GeoPoint type:geo_point is invalid, allow \\[long, double\\].*")
}

func (s *SearchSuite) TestAggregationSumAggregationInvalidTypeNested(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewSumAggregation("agg1", "Col_Nested"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[sum agg\\] field_name:Col_Nested type:nested is invalid, allow \\[long, double\\].*")
}

func (s *SearchSuite) TestAggregationSumAggregationUnknownField(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewSumAggregation("agg1", "Col_Unknown"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[sum agg\\] field: Col_Unknown does not exist.*")
}

/* count */

func (s *SearchSuite) TestAggregationCountAggregationEmptyAggName(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewCountAggregation("", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationCountAggregationValidType(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewCountAggregation("agg1", "Col_Long")).
			Aggregation(search.NewCountAggregation("agg2", "Col_Double")).
			Aggregation(search.NewCountAggregation("agg3", "Col_Boolean")).
			Aggregation(search.NewCountAggregation("agg4", "Col_Keyword")).
			Aggregation(search.NewCountAggregation("agg5", "Col_GeoPoint"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	{
		aggResult, err := searchResponse.AggregationResults.Count("agg1")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(10))
	}
	{
		aggResult, err := searchResponse.AggregationResults.Count("agg2")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(10))
	}
	{
		aggResult, err := searchResponse.AggregationResults.Count("agg3")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(10))
	}
	{
		aggResult, err := searchResponse.AggregationResults.Count("agg4")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(10))
	}
	{
		aggResult, err := searchResponse.AggregationResults.Count("agg5")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(10))
	}
}

func (s *SearchSuite) TestAggregationCountAggregationInvalidTypeNested(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewCountAggregation("agg1", "Col_Nested"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	//TODO wait for search-proxy update
	//_, err := client.Search(searchRequest)
	//c.Check(err.Error(), Matches, "OTSParameterInvalid invalid.*")
}

func (s *SearchSuite) TestAggregationCountAggregationUnknownField(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewCountAggregation("agg1", "Col_Unknown"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[count agg\\] field: Col_Unknown does not exist.*")
}

/* distinct count */

func (s *SearchSuite) TestAggregationDistinctCountAggregationEmptyAggName(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewDistinctCountAggregation("", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationDistinctCountAggregationValidType(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewDistinctCountAggregation("agg1", "Col_Long")).
			Aggregation(search.NewDistinctCountAggregation("agg2", "Col_Double")).
			Aggregation(search.NewDistinctCountAggregation("agg3", "Col_Boolean")).
			Aggregation(search.NewDistinctCountAggregation("agg4", "Col_Keyword")).
			Aggregation(search.NewDistinctCountAggregation("agg5", "Col_GeoPoint"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	{
		aggResult, err := searchResponse.AggregationResults.DistinctCount("agg1")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(10))
	}
	{
		aggResult, err := searchResponse.AggregationResults.DistinctCount("agg2")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(10))
	}
	{
		aggResult, err := searchResponse.AggregationResults.DistinctCount("agg3")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(2))
	}
	{
		aggResult, err := searchResponse.AggregationResults.DistinctCount("agg4")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(3))
	}
	{
		aggResult, err := searchResponse.AggregationResults.DistinctCount("agg5")
		c.Check(err, Equals, nil)
		c.Check(aggResult.Value, Equals, int64(10))
	}
}

func (s *SearchSuite) TestAggregationDistinctCountAggregationInvalidTypeNested(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewDistinctCountAggregation("agg1", "Col_Nested"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	//TODO wait for search-proxy update
	//_, err := client.Search(searchRequest)
	//c.Check(err.Error(), Matches, "OTSParameterInvalid invalid.*")
}

func (s *SearchSuite) TestAggregationDistinctCountAggregationUnknownField(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewDistinctCountAggregation("agg1", "Col_Unknown"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid \\[distinct count agg\\] field: Col_Unknown does not exist.*")
}

func (s *SearchSuite) TestAggregationSameLevelAggsBeyondLimit(c *C) {
	//should be no more than 5 agg in the same level
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Long")).
			Aggregation(search.NewAvgAggregation("agg2", "Col_Long")).
			Aggregation(search.NewAvgAggregation("agg3", "Col_Long")).
			Aggregation(search.NewAvgAggregation("agg4", "Col_Long")).
			Aggregation(search.NewAvgAggregation("agg4", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

func (s *SearchSuite) TestAggregationAggsSameNames(c *C) {
	//should be no more than 5 agg in the same level
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Long")).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	_, err := client.Search(searchRequest)
	c.Check(err.Error(), Matches, "OTSParameterInvalid.*")
}

// group by

func (s *SearchSuite) TestGroupByGroupByGetResultWrongType(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByFilter("group_by1").
				Query(&search.MatchAllQuery{}).
				Query(&search.TermQuery{
					FieldName: "Col_Keyword",
					Term:      "tablestore",
				}))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	_, err = searchResponse.GroupByResults.GroupByField("group_by1")
	c.Check(err.Error(), Matches, "wrong group by type.*")
}

func (s *SearchSuite) TestGroupByGroupByGetResultNotExist(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByFilter("group_by1").
				Query(&search.MatchAllQuery{}).
				Query(&search.TermQuery{
					FieldName: "Col_Keyword",
					Term:      "tablestore",
				}))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	_, err = searchResponse.GroupByResults.GroupByField("group_by_not_exist")
	c.Check(err.Error(), Matches, "group by.*not found")
}

func (s *SearchSuite) TestGroupByGroupByFieldValidType(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByField("group_by1", "Col_Long"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	//by default group order: (1)RowCountGroupBySort desc, (2)then GroupKeyGroupBySort asc
	groupByResult, err := searchResponse.GroupByResults.GroupByField("group_by1")
	c.Check(err, Equals, nil)

	c.Check(len(groupByResult.Items), Equals, 4)

	c.Check(int64(4), Equals, groupByResult.Items[0].RowCount)
	c.Check("4", Equals, groupByResult.Items[0].Key)
	c.Check(int64(3), Equals, groupByResult.Items[1].RowCount)
	c.Check("3", Equals, groupByResult.Items[1].Key)
	c.Check(int64(2), Equals, groupByResult.Items[2].RowCount)
	c.Check("2", Equals, groupByResult.Items[2].Key)
	c.Check(int64(1), Equals, groupByResult.Items[3].RowCount)
	c.Check("1", Equals, groupByResult.Items[3].Key)
}

func (s *SearchSuite) TestGroupByGroupByFieldSize(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByField("group_by1", "Col_Long").Size(2))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	//by default group order: (1)RowCountGroupBySort desc, (2)then GroupKeyGroupBySort asc
	groupByResult, err := searchResponse.GroupByResults.GroupByField("group_by1")
	c.Check(err, Equals, nil)

	c.Check(len(groupByResult.Items), Equals, 2)

	c.Check(int64(4), Equals, groupByResult.Items[0].RowCount)
	c.Check("4", Equals, groupByResult.Items[0].Key)
	c.Check(int64(3), Equals, groupByResult.Items[1].RowCount)
	c.Check("3", Equals, groupByResult.Items[1].Key)
}

func (s *SearchSuite) TestGroupByGroupByFieldSorters(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByField("group_by1", "Col_Keyword").
				GroupBySorters([]search.GroupBySorter{
					&search.SubAggGroupBySort{
						Order:      search.SortOrder_ASC.Enum(),
						SubAggName: "sub_agg1",
					},
					&search.GroupKeyGroupBySort{
						Order: search.SortOrder_DESC.Enum(),
					},
				}).
				SubAggregation(search.NewMinAggregation("sub_agg1", "Col_Long")))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	//by default group order: (1)RowCountGroupBySort desc, (2)then GroupKeyGroupBySort asc
	groupByResult, err := searchResponse.GroupByResults.GroupByField("group_by1")
	c.Check(err, Equals, nil)

	c.Check(len(groupByResult.Items), Equals, 2)

	c.Check(int64(4), Equals, groupByResult.Items[0].RowCount)
	c.Check("hangzhou", Equals, groupByResult.Items[0].Key)
	c.Check(int64(6), Equals, groupByResult.Items[1].RowCount)
	c.Check("tablestore", Equals, groupByResult.Items[1].Key)
}

func (s *SearchSuite) TestGroupByGroupByFilter(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByFilter("group_by1").
				Query(&search.MatchAllQuery{}).
				Query(&search.TermQuery{
					FieldName: "Col_Keyword",
					Term:      "tablestore",
				}))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	//by default group order: (1)RowCountGroupBySort desc, (2)then GroupKeyGroupBySort asc
	groupByResult, err := searchResponse.GroupByResults.GroupByFilter("group_by1")
	c.Check(err, Equals, nil)

	c.Check(len(groupByResult.Items), Equals, 2)

	c.Check(int64(10), Equals, groupByResult.Items[0].RowCount)
	c.Check(int64(6), Equals, groupByResult.Items[1].RowCount)
}

func (s *SearchSuite) TestGroupByGroupByGeoDistance(c *C) {
	searchRequest := &SearchRequest{}
	// 30.137817,120.08681 飞天
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByGeoDistance("group_by1", "Col_GeoPoint", search.GeoPoint{Lat: 30.137816, Lon: 120.08682}).
				Range(100, 5000).
				Range(math.Inf(-1), 5000).
				Range(5000, math.Inf(1)))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: true,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	groupByResult, err := searchResponse.GroupByResults.GroupByGeoDistance("group_by1")
	c.Check(err, Equals, nil)

	c.Check(len(groupByResult.Items), Equals, 3)

	c.Check(int64(1), Equals, groupByResult.Items[0].RowCount)
	c.Check(float64(100), Equals, groupByResult.Items[0].From)
	c.Check(float64(5000), Equals, groupByResult.Items[0].To)

	c.Check(int64(2), Equals, groupByResult.Items[1].RowCount)
	c.Check(float64(0), Equals, groupByResult.Items[1].From)
	c.Check(float64(5000), Equals, groupByResult.Items[1].To)

	c.Check(int64(8), Equals, groupByResult.Items[2].RowCount)
	c.Check(float64(5000), Equals, groupByResult.Items[2].From)
	c.Check(math.Inf(1), Equals, groupByResult.Items[2].To)
}

func (s *SearchSuite) TestGroupByGroupByRange(c *C) {
	searchRequest := &SearchRequest{}
	// 30.137817,120.08681 飞天
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByRange("group_by1", "Col_Double").
				Range(3, 4).
				Range(math.Inf(-1), 3).
				Range(4, math.Inf(1)))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: true,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)
	groupByResult, err := searchResponse.GroupByResults.GroupByRange("group_by1")
	c.Check(err, Equals, nil)

	c.Check(len(groupByResult.Items), Equals, 3)

	c.Check(int64(3), Equals, groupByResult.Items[0].RowCount)
	c.Check(math.Inf(-1), Equals, groupByResult.Items[0].From)
	c.Check(float64(3), Equals, groupByResult.Items[0].To)

	c.Check(int64(3), Equals, groupByResult.Items[1].RowCount)
	c.Check(float64(3), Equals, groupByResult.Items[1].From)
	c.Check(float64(4), Equals, groupByResult.Items[1].To)

	c.Check(int64(4), Equals, groupByResult.Items[2].RowCount)
	c.Check(float64(4), Equals, groupByResult.Items[2].From)
	c.Check(math.Inf(1), Equals, groupByResult.Items[2].To)
}

func (s *SearchSuite) TestGroupByNestedAggOrGroupBy(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByField("group_by1", "Col_Keyword").
				SubGroupBy(search.NewGroupByRange("sub_group_by1", "Col_Long").Range(math.Inf(-1), 3).Range(3, math.Inf(1))).
				SubAggregation(search.NewAvgAggregation("sub_agg1", "Col_Long")))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)

	groupByResult, err := searchResponse.GroupByResults.GroupByField("group_by1")
	c.Check(err, Equals, nil)

	//check outer
	c.Check(len(groupByResult.Items), Equals, 2)

	c.Check(int64(6), Equals, groupByResult.Items[0].RowCount)
	c.Check("tablestore", Equals, groupByResult.Items[0].Key)
	c.Check(int64(4), Equals, groupByResult.Items[1].RowCount)
	c.Check("hangzhou", Equals, groupByResult.Items[1].Key)

	//check inner agg
	{
		subAgg1 := groupByResult.Items[0].SubAggregations
		subAggResult1, _ := subAgg1.Avg("sub_agg1")
		c.Check(subAggResult1.Value, Equals, float64(11)/3)
	}
	{
		subAgg1 := groupByResult.Items[1].SubAggregations
		subAggResult1, _ := subAgg1.Avg("sub_agg1")
		c.Check(subAggResult1.Value, Equals, float64(2))
	}

	//check inner group by
	{
		subGroupBy1, _ := groupByResult.Items[0].SubGroupBys.GroupByRange("sub_group_by1")
		c.Check(2, Equals, len(subGroupBy1.Items))

		c.Check(math.Inf(-1), Equals, subGroupBy1.Items[0].From)
		c.Check(float64(3), Equals, subGroupBy1.Items[0].To)
		c.Check(int64(0), Equals, subGroupBy1.Items[0].RowCount)

		c.Check(float64(3), Equals, subGroupBy1.Items[1].From)
		c.Check(math.Inf(1), Equals, subGroupBy1.Items[1].To)
		c.Check(int64(6), Equals, subGroupBy1.Items[1].RowCount)
	}
	{
		subGroupBy1, _ := groupByResult.Items[1].SubGroupBys.GroupByRange("sub_group_by1")
		c.Check(2, Equals, len(subGroupBy1.Items))

		c.Check(math.Inf(-1), Equals, subGroupBy1.Items[0].From)
		c.Check(float64(3), Equals, subGroupBy1.Items[0].To)
		c.Check(int64(3), Equals, subGroupBy1.Items[0].RowCount)

		c.Check(float64(3), Equals, subGroupBy1.Items[1].From)
		c.Check(math.Inf(1), Equals, subGroupBy1.Items[1].To)
		c.Check(int64(1), Equals, subGroupBy1.Items[1].RowCount)
	}
}

func (s *SearchSuite) TestGroupByNestedField(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			Aggregation(search.NewAvgAggregation("agg1", "Col_Nested.Col_Long_Nested")).
			GroupBy(search.NewGroupByField("group_by1", "Col_Nested.Col_Long_Nested"))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)

	aggResult, err := searchResponse.AggregationResults.Avg("agg1")
	c.Check(err, Equals, nil)
	c.Check(aggResult.Value, Equals, float64(3))

	groupByResult, err := searchResponse.GroupByResults.GroupByField("group_by1")
	c.Check(err, Equals, nil)

	//check outer
	c.Check(len(groupByResult.Items), Equals, 4)

	c.Check("4", Equals, groupByResult.Items[0].Key)
	c.Check(int64(4), Equals, groupByResult.Items[0].RowCount)
	c.Check(true, Equals, groupByResult.Items[0].SubAggregations.Empty())
	c.Check(true, Equals, groupByResult.Items[0].SubGroupBys.Empty())

	c.Check("3", Equals, groupByResult.Items[1].Key)
	c.Check(int64(3), Equals, groupByResult.Items[1].RowCount)
	c.Check(true, Equals, groupByResult.Items[1].SubAggregations.Empty())
	c.Check(true, Equals, groupByResult.Items[1].SubGroupBys.Empty())

	c.Check("2", Equals, groupByResult.Items[2].Key)
	c.Check(int64(2), Equals, groupByResult.Items[2].RowCount)
	c.Check(true, Equals, groupByResult.Items[2].SubAggregations.Empty())
	c.Check(true, Equals, groupByResult.Items[2].SubGroupBys.Empty())

	c.Check("1", Equals, groupByResult.Items[3].Key)
	c.Check(int64(1), Equals, groupByResult.Items[3].RowCount)
	c.Check(true, Equals, groupByResult.Items[3].SubAggregations.Empty())
	c.Check(true, Equals, groupByResult.Items[3].SubGroupBys.Empty())
}

func (s *SearchSuite) TestGroupByNestedFieldUnderGroupBy(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName2).
		SetIndexName(searchAPITestIndexName2).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByField("group_by1", "Col_Keyword").
				SubAggregation(search.NewAvgAggregation("sub_agg1", "Col_Nested.Col_Long_Nested")).
				SubGroupBy(search.NewGroupByField("sub_group_by1", "Col_Nested.Col_Long_Nested")))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: false,
		})
	searchResponse, err := client.Search(searchRequest)
	c.Check(err, Equals, nil)

	groupByResult, err := searchResponse.GroupByResults.GroupByField("group_by1")
	c.Check(err, Equals, nil)

	//check outer
	c.Check(len(groupByResult.Items), Equals, 2)

	c.Check(int64(6), Equals, groupByResult.Items[0].RowCount)
	c.Check("tablestore", Equals, groupByResult.Items[0].Key)
	c.Check(int64(4), Equals, groupByResult.Items[1].RowCount)
	c.Check("hangzhou", Equals, groupByResult.Items[1].Key)

	//check inner agg
	{
		subAgg1 := groupByResult.Items[0].SubAggregations
		subAggResult1, _ := subAgg1.Avg("sub_agg1")
		c.Check(subAggResult1.Value, Equals, float64(11)/3)
	}
	{
		subAgg1 := groupByResult.Items[1].SubAggregations
		subAggResult1, _ := subAgg1.Avg("sub_agg1")
		c.Check(subAggResult1.Value, Equals, float64(2))
	}

	//check inner group by
	{
		subGroupBy1, _ := groupByResult.Items[0].SubGroupBys.GroupByField("sub_group_by1")
		c.Check(2, Equals, len(subGroupBy1.Items))

		c.Check("4", Equals, subGroupBy1.Items[0].Key)
		c.Check(int64(4), Equals, subGroupBy1.Items[0].RowCount)
		c.Check(true, Equals, subGroupBy1.Items[0].SubAggregations.Empty())
		c.Check(true, Equals, subGroupBy1.Items[0].SubGroupBys.Empty())

		c.Check("3", Equals, subGroupBy1.Items[1].Key)
		c.Check(int64(2), Equals, subGroupBy1.Items[1].RowCount)
		c.Check(true, Equals, subGroupBy1.Items[1].SubAggregations.Empty())
		c.Check(true, Equals, subGroupBy1.Items[1].SubGroupBys.Empty())
	}
	{
		subGroupBy1, _ := groupByResult.Items[1].SubGroupBys.GroupByField("sub_group_by1")
		c.Check(3, Equals, len(subGroupBy1.Items))

		c.Check("2", Equals, subGroupBy1.Items[0].Key)
		c.Check(int64(2), Equals, subGroupBy1.Items[0].RowCount)
		c.Check(true, Equals, subGroupBy1.Items[0].SubAggregations.Empty())
		c.Check(true, Equals, subGroupBy1.Items[0].SubGroupBys.Empty())

		c.Check("1", Equals, subGroupBy1.Items[1].Key)
		c.Check(int64(1), Equals, subGroupBy1.Items[1].RowCount)
		c.Check(true, Equals, subGroupBy1.Items[1].SubAggregations.Empty())
		c.Check(true, Equals, subGroupBy1.Items[1].SubGroupBys.Empty())

		c.Check("3", Equals, subGroupBy1.Items[2].Key)
		c.Check(int64(1), Equals, subGroupBy1.Items[2].RowCount)
		c.Check(true, Equals, subGroupBy1.Items[2].SubAggregations.Empty())
		c.Check(true, Equals, subGroupBy1.Items[2].SubGroupBys.Empty())
	}
}

func (s *SearchSuite) TestSearchQueryWithHighlight(c *C) {
	searchRequest := &SearchRequest{}
	searchRequest.
		SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchQuery{
				FieldName: "Col_Highlight_Text",
				Text:      "xihu dengcai",
			}).
			SetHighlight(search.NewHighlight().
				SetHighlightEncoder(search.HtmlMode).
				AddFieldHighlightParameter("Col_Highlight_Text", search.NewHighlightParameter().
					SetPreTag("<em>").
					SetPostTag("</em>").
					SetFragmentSize(100).
					SetHighlightFragmentOrder(search.TextSequence).
					SetNumberOfFragments(5)))).
		SetColumnsToGet(&ColumnsToGet{
			ReturnAll: true,
		})
	resp, err := client.Search(searchRequest)
	c.Check(err, IsNil)
	c.Check(resp.SearchHits, NotNil)
	c.Check(resp.SearchHits[0].HighlightResultItem.HighlightFields["Col_Highlight_Text"], NotNil)
	c.Check(strings.Contains(resp.SearchHits[0].HighlightResultItem.HighlightFields["Col_Highlight_Text"].Fragments[0], "<em>xihu</em>"), Equals, true)
	c.Check(strings.Contains(resp.SearchHits[0].HighlightResultItem.HighlightFields["Col_Highlight_Text"].Fragments[0], "&lt;em&gt;street&lt;&#x2F;em&gt;"), Equals, true)
}

/* compute splits */
func (s *SearchSuite) TestComputeSplits(c *C) {
	req := &ComputeSplitsRequest{}
	req.
		SetTableName(searchAPITestTableName1).
		SetSearchIndexSplitsOptions(SearchIndexSplitsOptions{IndexName: searchAPITestIndexName1})
	res, err := client.ComputeSplits(req)
	c.Check(err, Equals, nil)
	c.Check(int32(1), Equals, res.SplitsSize)
}

func (s *SearchSuite) TestComputeSplitsInvalidTableName(c *C) {
	req := &ComputeSplitsRequest{}
	req.SetTableName("invalid_table_name").
		SetSearchIndexSplitsOptions(SearchIndexSplitsOptions{IndexName: searchAPITestIndexName1})
	_, err := client.ComputeSplits(req)
	c.Check(err.Error(), Matches, "OTSParameterInvalid table \\[invalid_table_name\\] does not exist.*")
}

func (s *SearchSuite) TestComputeSplitsInvalidIndexName(c *C) {
	req := &ComputeSplitsRequest{}
	req.
		SetTableName(searchAPITestTableName1).
		SetSearchIndexSplitsOptions(SearchIndexSplitsOptions{IndexName: "invalid_index_name"})
	_, err := client.ComputeSplits(req)
	c.Check(err.Error(), Matches, "OTSMetaNotMatch.*")
}

func computeSplits(tableName string, indexName string) (*ComputeSplitsResponse, error) {
	req := &ComputeSplitsRequest{}
	req.
		SetTableName(tableName).
		SetSearchIndexSplitsOptions(SearchIndexSplitsOptions{IndexName: indexName})
	res, err := client.ComputeSplits(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *SearchSuite) TestParallelScanSingleThread(c *C) {
	computeSplitsResp, err := computeSplits(searchAPITestTableName1, searchAPITestIndexName1)
	c.Check(err, Equals, nil)

	query := search.NewScanQuery().SetQuery(&search.MatchAllQuery{}).SetLimit(2)

	req := &ParallelScanRequest{}
	req.SetTableName(searchAPITestTableName1).
		SetIndexName(searchAPITestIndexName1).
		SetColumnsToGet(&ColumnsToGet{ReturnAllFromIndex: false}).
		SetScanQuery(query).
		SetSessionId(computeSplitsResp.SessionId)

	res, err := client.ParallelScan(req)
	c.Check(err, Equals, nil)

	total := len(res.Rows)
	for res.NextToken != nil {
		req.SetScanQuery(query.SetToken(res.NextToken))
		res, err = client.ParallelScan(req)
		c.Check(err, Equals, nil)

		total += len(res.Rows) //process rows each loop
	}
	c.Check(total, Equals, 10)
}

func (s *SearchSuite) TestParallelScanMultiThread(c *C) {
	//reindex to more than 1 shard
	computeSplitsResp, err := computeSplits(searchAPITestTableName1, searchAPITestIndexName1)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

	var lock sync.Mutex
	total := 0

	var wg sync.WaitGroup
	wg.Add(int(computeSplitsResp.SplitsSize))

	for i := int32(0); i < computeSplitsResp.SplitsSize; i++ {
		current := i
		go func() {
			defer wg.Done()
			query := search.NewScanQuery().
				SetQuery(&search.MatchAllQuery{}).
				SetCurrentParallelID(current).
				SetMaxParallel(computeSplitsResp.SplitsSize).
				SetLimit(2)

			req := &ParallelScanRequest{}
			req.SetTableName(searchAPITestTableName1).
				SetIndexName(searchAPITestIndexName1).
				SetColumnsToGet(&ColumnsToGet{ReturnAllFromIndex: false}).
				SetScanQuery(query).
				SetSessionId(computeSplitsResp.SessionId)

			res, err := client.ParallelScan(req)
			if err != nil {
				fmt.Printf("%#v", err)
				return
			}

			lock.Lock()
			total += len(res.Rows)
			lock.Unlock()

			for res.NextToken != nil {
				req.SetScanQuery(query.SetToken(res.NextToken))
				res, err = client.ParallelScan(req)
				if err != nil {
					fmt.Printf("%#v", err)
					return
				}

				lock.Lock()
				total += len(res.Rows)
				lock.Unlock()
			}
		}()
	}
	wg.Wait()

	c.Check(total, Equals, 10)
}

func (s *SearchSuite) TestCreateSearchIndexWithDateFieldAndDescribeSearchIndex(c *C) {
	tableName := "go_sdk_test_table"
	indexName := "go_sdk_test_index"
	DeleteTableAndAllIndex(c, client, tableName)
	CreateSearchTable(c, client, tableName)
	indexSchema := getNormalTestIndexSchemaWithNested()
	CreateSearchIndex(c, client, tableName, indexName, nil, indexSchema, -1)
	resp := DescribeSearchIndex(c, client, tableName, indexName)
	c.Check(resp, NotNil)
	fieldSchemas := resp.Schema.FieldSchemas
	c.Check(fieldSchemas, NotNil)
	// check date field
	c.Check(len(fieldSchemas), Equals, len(indexSchema.FieldSchemas))

	c.Check(fieldSchemas[0].FieldType, Equals, indexSchema.FieldSchemas[0].FieldType)
	c.Check(fmt.Sprintf("%s", fieldSchemas[0].DateFormats), Equals, fmt.Sprintf("%s", indexSchema.FieldSchemas[0].DateFormats))
	c.Check(*fieldSchemas[0].FieldName, Equals, *indexSchema.FieldSchemas[0].FieldName)

	c.Check(fieldSchemas[1].FieldType, Equals, indexSchema.FieldSchemas[1].FieldType)
	c.Check(*fieldSchemas[1].FieldName, Equals, *indexSchema.FieldSchemas[1].FieldName)

	c.Check(len(fieldSchemas[1].FieldSchemas), Equals, len(indexSchema.FieldSchemas[1].FieldSchemas))

	c.Check(fieldSchemas[1].FieldSchemas[0].FieldType, Equals, indexSchema.FieldSchemas[1].FieldSchemas[0].FieldType)
	c.Check(fmt.Sprintf("%s", fieldSchemas[1].FieldSchemas[0].DateFormats), Equals, fmt.Sprintf("%s", indexSchema.FieldSchemas[1].FieldSchemas[0].DateFormats))
	c.Check(*fieldSchemas[1].FieldSchemas[0].FieldName, Equals, *indexSchema.FieldSchemas[1].FieldSchemas[0].FieldName)
}

func (s *SearchSuite) TestCreateSearchIndexWithTTLAndDescribeSearchIndex(c *C) {
	tableName := "go_sdk_test_table_ttl"
	indexName := "go_sdk_test_index"
	DeleteTableAndAllIndex(c, client, tableName)
	CreateSearchTableAndDisallowUpdate(c, client, tableName)
	indexSchema := getNormalTestIndexSchema()
	ttl := int32(864000)
	CreateSearchIndex(c, client, tableName, indexName, nil, indexSchema, ttl)
	resp := DescribeSearchIndex(c, client, tableName, indexName)
	c.Check(resp.TimeToLive, Equals, ttl)
	c.Check(resp, NotNil)
	fieldSchemas := resp.Schema.FieldSchemas
	c.Check(fieldSchemas, NotNil)
	// check date field
	c.Check(len(fieldSchemas), Equals, len(indexSchema.FieldSchemas))
	c.Check(fieldSchemas[0].FieldType, Equals, indexSchema.FieldSchemas[0].FieldType)
	c.Check(*fieldSchemas[0].FieldName, Equals, *indexSchema.FieldSchemas[0].FieldName)
}

func (s *SearchSuite) TestCreateSearchIndexWithHighlightingField(c *C) {
	tableName := "go_sdk_test_highlighting_table"
	indexName := "go_sdk_test_highlighting_index"
	DeleteTableAndAllIndex(c, client, tableName)
	CreateSearchTable(c, client, tableName)
	indexSchema := getNormalTestIndexSchema()
	CreateSearchIndex(c, client, tableName, indexName, nil, indexSchema, -1)
	resp := DescribeSearchIndex(c, client, tableName, indexName)
	c.Check(resp, NotNil)
	fieldSchemas := resp.Schema.FieldSchemas
	c.Check(fieldSchemas, NotNil)
	c.Check(fieldSchemas[2].FieldType.String(), Equals, indexSchema.FieldSchemas[2].FieldType.String())
	c.Check(*fieldSchemas[2].EnableHighlighting, Equals, *indexSchema.FieldSchemas[2].EnableHighlighting)
}

func (s *SearchSuite) TestUpdateSearchIndexTTL(c *C) {
	tableName := "go_sdk_test_update_search_index"
	indexName := "go_sdk_test_index"
	//init
	{
		DeleteTableAndAllIndex(c, client, tableName)
		CreateSearchTableAndDisallowUpdate(c, client, tableName)
		indexSchema := getNormalTestIndexSchema()
		ttl := int32(-1)
		CreateSearchIndex(c, client, tableName, indexName, nil, indexSchema, ttl)
		respD := DescribeSearchIndex(c, client, tableName, indexName)
		c.Check(respD.TimeToLive, Equals, ttl)
		c.Check(respD, NotNil)
	}
	//update ttl
	{
		updateTTL := int32(3600 * 24 * 7)
		req := new(UpdateSearchIndexRequest)
		req.TableName = tableName
		req.IndexName = indexName
		req.TimeToLive = proto.Int32(updateTTL)
		resp, err := client.UpdateSearchIndex(req)
		if err != nil {
			c.Log(fmt.Println(err))
		}
		c.Log("UpdateSearchIndex", resp.ResponseInfo.RequestId)
		c.Check(err, IsNil)
		respD := DescribeSearchIndex(c, client, tableName, indexName)
		c.Check(respD.TimeToLive, Equals, updateTTL)
		c.Check(respD, NotNil)
	}
}

func (s *SearchSuite) TestUpdateSchemaAndDescribeSearchIndex(c *C) {
	tableName := "go_sdk_test_table"
	indexName := "go_sdk_test_index"
	// 修改schema后的索引, 索引必须以_reindex结尾
	indexReindexName := "go_sdk_test_index_reindex"
	indexSchema := getNormalTestIndexSchema()
	indexReindexSchema := getNormalTestIndexSchemaWithNested()

	{
		// step 1.创建索引
		DeleteTableAndAllIndex(c, client, tableName)
		CreateSearchTable(c, client, tableName)
		CreateSearchIndex(c, client, tableName, indexName, nil, indexSchema, -1)
		resp := DescribeSearchIndex(c, client, tableName, indexName)
		c.Check(resp, NotNil)
		fieldSchemas := resp.Schema.FieldSchemas
		c.Check(fieldSchemas, NotNil)
		c.Check(len(fieldSchemas), Equals, len(indexSchema.FieldSchemas))
		c.Check(fieldSchemas[0].FieldType, Equals, indexSchema.FieldSchemas[0].FieldType)
		c.Check(*fieldSchemas[0].FieldName, Equals, *indexSchema.FieldSchemas[0].FieldName)
	}
	{
		// step 2.创建修改schema后的索引
		CreateSearchIndex(c, client, tableName, indexReindexName, &indexName, indexReindexSchema, -1)
		resp := DescribeSearchIndex(c, client, tableName, indexReindexName)
		c.Check(resp, NotNil)
		fieldSchemas := resp.Schema.FieldSchemas
		c.Check(fieldSchemas, NotNil)
		c.Check(len(fieldSchemas), Equals, len(indexReindexSchema.FieldSchemas))
		c.Check(fieldSchemas[0].FieldType, Equals, indexReindexSchema.FieldSchemas[0].FieldType)
		c.Check(fmt.Sprintf("%s", fieldSchemas[0].DateFormats), Equals, fmt.Sprintf("%s", indexReindexSchema.FieldSchemas[0].DateFormats))
		c.Check(*fieldSchemas[0].FieldName, Equals, *indexReindexSchema.FieldSchemas[0].FieldName)
		c.Check(fieldSchemas[1].FieldType, Equals, indexReindexSchema.FieldSchemas[1].FieldType)
		c.Check(*fieldSchemas[1].FieldName, Equals, *indexReindexSchema.FieldSchemas[1].FieldName)
		c.Check(len(fieldSchemas[1].FieldSchemas), Equals, len(indexReindexSchema.FieldSchemas[1].FieldSchemas))
		c.Check(fieldSchemas[1].FieldSchemas[0].FieldType, Equals, indexReindexSchema.FieldSchemas[1].FieldSchemas[0].FieldType)
		c.Check(fmt.Sprintf("%s", fieldSchemas[1].FieldSchemas[0].DateFormats), Equals, fmt.Sprintf("%s", indexReindexSchema.FieldSchemas[1].FieldSchemas[0].DateFormats))
		c.Check(*fieldSchemas[1].FieldSchemas[0].FieldName, Equals, *indexReindexSchema.FieldSchemas[1].FieldSchemas[0].FieldName)
	}
	{
		// step 3.设置AB索引权重，权重在0-100，此处原索引权重为0 新索引权重为100
		fmt.Println("wait schema reload")
		time.Sleep(60 * time.Second)
		req := new(UpdateSearchIndexRequest)
		req.TableName = tableName
		req.IndexName = indexName
		queryFlowWeightArray := make([]*QueryFlowWeight, 0)
		queryFlowWeightArray = append(queryFlowWeightArray, &QueryFlowWeight{
			IndexName: indexName,
			Weight:    0,
		})
		queryFlowWeightArray = append(queryFlowWeightArray, &QueryFlowWeight{
			IndexName: indexReindexName,
			Weight:    100,
		})
		req.QueryFlowWeights = queryFlowWeightArray
		resp, err := client.UpdateSearchIndex(req)
		if err != nil {
			c.Log(fmt.Println(err))
		}
		c.Log("UpdateSearchIndex", resp.ResponseInfo.RequestId)
		c.Check(err, IsNil)
		respD := DescribeSearchIndex(c, client, tableName, indexName)
		c.Check(respD, NotNil)
		queryFlowWeights := respD.QueryFlowWeights
		c.Check(queryFlowWeights[0].IndexName, Equals, queryFlowWeightArray[0].IndexName)
		c.Check(queryFlowWeights[0].Weight, Equals, queryFlowWeightArray[0].Weight)
		c.Check(queryFlowWeights[1].IndexName, Equals, queryFlowWeightArray[1].IndexName)
		c.Check(queryFlowWeights[1].Weight, Equals, queryFlowWeightArray[1].Weight)
	}

	{
		// step 4.切换索引, 此时索引schema变为新索引的schema
		switchReq := new(UpdateSearchIndexRequest)
		switchReq.TableName = tableName
		switchReq.IndexName = indexName
		switchReq.SwitchIndexName = &indexReindexName
		resp, err := client.UpdateSearchIndex(switchReq)
		if err != nil {
			c.Log(fmt.Println(err))
		}
		c.Log("UpdateSearchIndex", resp.ResponseInfo.RequestId)
		c.Check(err, IsNil)
		respD := DescribeSearchIndex(c, client, tableName, indexName)
		c.Check(respD, NotNil)
		fieldSchemas := respD.Schema.FieldSchemas
		c.Check(fieldSchemas, NotNil)
		c.Check(len(fieldSchemas), Equals, len(indexReindexSchema.FieldSchemas))
		c.Check(fieldSchemas[0].FieldType, Equals, indexReindexSchema.FieldSchemas[0].FieldType)
		c.Check(fmt.Sprintf("%s", fieldSchemas[0].DateFormats), Equals, fmt.Sprintf("%s", indexReindexSchema.FieldSchemas[0].DateFormats))
		c.Check(*fieldSchemas[0].FieldName, Equals, *indexReindexSchema.FieldSchemas[0].FieldName)
		c.Check(fieldSchemas[1].FieldType, Equals, indexReindexSchema.FieldSchemas[1].FieldType)
		c.Check(*fieldSchemas[1].FieldName, Equals, *indexReindexSchema.FieldSchemas[1].FieldName)
		c.Check(len(fieldSchemas[1].FieldSchemas), Equals, len(indexReindexSchema.FieldSchemas[1].FieldSchemas))
		c.Check(fieldSchemas[1].FieldSchemas[0].FieldType, Equals, indexReindexSchema.FieldSchemas[1].FieldSchemas[0].FieldType)
		c.Check(fmt.Sprintf("%s", fieldSchemas[1].FieldSchemas[0].DateFormats), Equals, fmt.Sprintf("%s", indexReindexSchema.FieldSchemas[1].FieldSchemas[0].DateFormats))
		c.Check(*fieldSchemas[1].FieldSchemas[0].FieldName, Equals, *indexReindexSchema.FieldSchemas[1].FieldSchemas[0].FieldName)
	}
	{
		// step 5.经过一段静默时间后，可以删除修改前的索引
		DeleteIndex(c, client, tableName, indexReindexName)
	}
}
