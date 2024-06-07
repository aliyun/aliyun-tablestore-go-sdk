package sample

import (
	"encoding/json"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search/model"
	"github.com/golang/protobuf/proto"
	"strconv"
	"strings"
	"sync"
	"time"
)

/**
 *创建一个SearchIndex，包含Col_Keyword和Col_Long两列，类型分别设置为字符串(KEYWORD)和整型(LONG)。
 */
func CreateSearchIndex(client *tablestore.TableStoreClient, tableName string, indexName string) {
	fmt.Println("Begin to create table:", tableName)
	createtableRequest := new(tablestore.CreateTableRequest)

	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_STRING)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput

	_, err := client.CreateTable(createtableRequest)
	if err != nil {
		fmt.Println("Failed to create table with error:", err)
	} else {
		fmt.Println("Create table finished")
	}

	fmt.Println("Begin to create index:", indexName)
	request := &tablestore.CreateSearchIndexRequest{}
	request.TableName = tableName // 设置表名
	request.IndexName = indexName // 设置索引名

	schemas := []*tablestore.FieldSchema{}
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:        tablestore.FieldType_KEYWORD, // 设置字段类型
		Index:            proto.Bool(true),             // 设置开启索引
		EnableSortAndAgg: proto.Bool(true),             // 设置开启排序与统计功能
	}
	field2 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Long"),
		FieldType:        tablestore.FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	schemas = append(schemas, field1, field2)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // 设置SearchIndex包含的字段
	}
	resp, err := client.CreateSearchIndex(request) // 调用client创建SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
}

/**
 *创建一个含虚拟列SearchIndex
 *包含Col_Keyword和Col_Long两个基础列，类型分别设置为字符串(KEYWORD)和整型(LONG)。
 *Col_long_str 为虚拟列，类型为字符串（KEYWORD）映射原始列为Col_long
 */
func CreateSearchIndexWithVirtualField(client *tablestore.TableStoreClient, tableName string, indexName string) {
	fmt.Println("Begin to create table:", tableName)
	createtableRequest := new(tablestore.CreateTableRequest)

	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_STRING)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput

	_, err := client.CreateTable(createtableRequest)
	if err != nil {
		fmt.Println("Failed to create table with error:", err)
	} else {
		fmt.Println("Create table finished")
	}

	fmt.Println("Begin to create index:", indexName)
	request := &tablestore.CreateSearchIndexRequest{}
	request.TableName = tableName // 设置表名
	request.IndexName = indexName // 设置索引名

	schemas := []*tablestore.FieldSchema{}
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:        tablestore.FieldType_KEYWORD, // 设置字段类型
		Index:            proto.Bool(true),             // 设置开启索引
		EnableSortAndAgg: proto.Bool(true),             // 设置开启排序与统计功能
	}
	field2 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Long"),
		FieldType:        tablestore.FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field3 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Long_str"),
		FieldType:        tablestore.FieldType_KEYWORD,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
		IsVirtualField:   proto.Bool(true),     //设置字段类型为虚拟列
		SourceFieldNames: []string{"Col_Long"}, //设置虚拟列映射的原始列
	}
	schemas = append(schemas, field1, field2, field3)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // 设置SearchIndex包含的字段
	}
	resp, err := client.CreateSearchIndex(request) // 调用client创建SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
}

/**
 *创建一个SearchIndex，包含Col_Keyword和Col_Long两列，类型分别设置为字符串(KEYWORD)和整型(LONG)，设置按照Col_Long这一列预先排序。
 */
func CreateSearchIndexWithIndexSort(client *tablestore.TableStoreClient, tableName string, indexName string) {
	fmt.Println("Begin to create index:", indexName)
	request := &tablestore.CreateSearchIndexRequest{}
	request.TableName = tableName // 设置表名
	request.IndexName = indexName // 设置索引名

	schemas := []*tablestore.FieldSchema{}
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:        tablestore.FieldType_KEYWORD, // 设置字段类型
		Index:            proto.Bool(true),             // 设置开启索引
		EnableSortAndAgg: proto.Bool(true),             // 设置开启排序与统计功能
	}
	field2 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Long"),
		FieldType:        tablestore.FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	schemas = append(schemas, field1, field2)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // 设置SearchIndex包含的字段
		IndexSort: &search.Sort{ // 设置indexsort，按照Col_Long的值逆序排序
			Sorters: []search.Sorter{
				&search.FieldSort{
					FieldName: "Col_Long",
					Order:     search.SortOrder_ASC.Enum(),
				},
			},
		},
	}
	resp, err := client.CreateSearchIndex(request) // 调用client创建SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
}

// 创建一个SearchIndex，为查询高亮Demo做正准备
func CreateSearchIndexForQueryHighlighting(client *tablestore.TableStoreClient, tableName string, indexName string) {
	var schemas []*tablestore.FieldSchema
	field1 := &tablestore.FieldSchema{
		FieldName:          proto.String("Col_Text"),  // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:          tablestore.FieldType_TEXT, // 设置字段类型
		Index:              proto.Bool(true),          // 设置开启索引
		EnableHighlighting: proto.Bool(true),          // 设置开启字段高亮
	}
	field2 := &tablestore.FieldSchema{
		FieldName: proto.String("Col_Nested"),
		FieldType: tablestore.FieldType_NESTED,
		FieldSchemas: []*tablestore.FieldSchema{
			{
				FieldName:          proto.String("Level1_Text"),
				FieldType:          tablestore.FieldType_TEXT,
				Index:              proto.Bool(true),
				EnableHighlighting: proto.Bool(true),
			},
			{
				FieldName: proto.String("Level1_Nested"),
				FieldType: tablestore.FieldType_NESTED,
				FieldSchemas: []*tablestore.FieldSchema{
					{
						FieldName:          proto.String("Level2_Text"),
						FieldType:          tablestore.FieldType_TEXT,
						Index:              proto.Bool(true),
						EnableHighlighting: proto.Bool(true),
					},
				},
			},
		},
	}
	schemas = append(schemas, field1, field2)

	createSearchIndex(client, tableName, indexName, schemas)
}

// CreateSearchIndexForVectorQuery: Create Searchindex with vector field.
func CreateSearchIndexForVectorQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("col_keyword"),
		FieldType:        tablestore.FieldType_KEYWORD,
		Index:            proto.Bool(true),
		Store:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field2 := &tablestore.FieldSchema{
		FieldName: proto.String("col_text"),
		FieldType: tablestore.FieldType_TEXT,
		Index:     proto.Bool(true),
	}
	field3 := &tablestore.FieldSchema{
		FieldName: proto.String("col_vector"),
		FieldType: tablestore.FieldType_VECTOR,
		Index:     proto.Bool(true),
		VectorOptions: &tablestore.VectorOptions{
			Dimension:        proto.Int32(8),
			VectorDataType:   tablestore.VectorDataType_FLOAT_32.Enum(),
			VectorMetricType: tablestore.VectorMetricType_DOT_PRODUCT.Enum(),
		},
	}
	createSearchIndex(client, tableName, indexName, []*tablestore.FieldSchema{field1, field2, field3})
}

// WriteDataForVectorQuery 为高亮查询测试插入数据
func WriteDataForVectorQuery(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("Begin to write data")
	keyword := []string{"tablestore", "searchindex", "vectorquery"}
	text := []string{"table store", "search index", "vector query"}
	for i := 0; i < 100; i++ {
		putPK := new(tablestore.PrimaryKey)
		putPK.AddPrimaryKeyColumn("pk1", strconv.Itoa(i))
		putRowChange := new(tablestore.PutRowChange)
		putRowChange.TableName = tableName
		putRowChange.PrimaryKey = putPK
		putRowChange.AddColumn("col_keyword", keyword[i%len(keyword)])
		putRowChange.AddColumn("col_text", text[i%len(text)])
		putRowChange.AddColumn("col_vector", fmt.Sprintf("[%f, %f, %f, %f, %f, %f, %f, %f]", float32(i)+1.5, float32(i)-1.5, float32(i)+5.5, float32(i)-5.5, float32(i)+10.5, float32(i)-10.5, float32(i)+20.5, float32(i)-20.5))
		putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		putRowRequest := new(tablestore.PutRowRequest)
		putRowRequest.PutRowChange = putRowChange
		if _, err := client.PutRow(putRowRequest); err != nil {
			fmt.Println("Put test data failed with err: ", err)
		}
	}
	time.Sleep(30 * time.Second)
	fmt.Println("Write data finished.")
}
func VectorQuerySample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	fmt.Println("Begin to run vector query")
	searchQuery := search.NewSearchQuery()
	float32VectorQuery := &search.KnnVectorQuery{
		FieldName: "col_vector",
		TopK:      proto.Int32(10),
		Filter: &search.BoolQuery{
			ShouldQueries: []search.Query{
				&search.TermQuery{
					FieldName: "col_keyword",
					Term:      "vectorquery",
				},
				&search.MatchQuery{
					FieldName: "col_text",
					Text:      "search",
				},
			},
		},
		Float32QueryVector: []float32{1.5, -1.5, 5.5, -5.5, 10.5, -10.5, 20.5, -20.5},
	}
	searchQuery.Query = float32VectorQuery
	searchQuery.Sort = &search.Sort{
		Sorters: []search.Sorter{
			search.NewScoreSort(),
		},
	}
	searchRequest := &tablestore.SearchRequest{
		SearchQuery:  searchQuery,
		TableName:    tableName,
		IndexName:    indexName,
		ColumnsToGet: &tablestore.ColumnsToGet{ReturnAllFromIndex: true},
	}

	if resp, err := client.Search(searchRequest); err != nil {
		fmt.Println("float32 vector query failed: " + err.Error())
	} else {
		for _, row := range resp.SearchHits {
			fmt.Printf("PK: %v ", row.Row.PrimaryKey.PrimaryKeys)
			fmt.Printf("Column: [")
			for _, column := range row.Row.Columns {
				fmt.Printf("{Name: %v, Value: %v}", column.ColumnName, column.Value)
			}
			fmt.Println("]")
		}
	}
	fmt.Println("Vector query sample finished")
}

func CreateSearchIndexForSearchQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	var schemas []*tablestore.FieldSchema
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:        tablestore.FieldType_KEYWORD, // 设置字段类型
		Index:            proto.Bool(true),             // 设置开启索引
		EnableSortAndAgg: proto.Bool(true),             // 设置开启排序与统计功能
	}

	schemas = append(schemas, field1)

	createSearchIndex(client, tableName, indexName, schemas)
}

/**
 *创建一个SearchIndex，为Aggregation和GroupBy的demo做准备
 */
func CreateSearchIndexForAggregationAndGroupBy(client *tablestore.TableStoreClient, tableName string, indexName string) {
	var schemas []*tablestore.FieldSchema
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:        tablestore.FieldType_KEYWORD, // 设置字段类型
		Index:            proto.Bool(true),             // 设置开启索引
		EnableSortAndAgg: proto.Bool(true),             // 设置开启排序与统计功能
	}
	field2 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword2"), // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:        tablestore.FieldType_KEYWORD, // 设置字段类型
		Index:            proto.Bool(true),             // 设置开启索引
		EnableSortAndAgg: proto.Bool(true),             // 设置开启排序与统计功能
	}
	field3 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Long"),
		FieldType:        tablestore.FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field4 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_GeoPoint"),
		FieldType:        tablestore.FieldType_GEO_POINT,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field5 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Double"),
		FieldType:        tablestore.FieldType_DOUBLE,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	field6 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Date"),
		FieldType:        tablestore.FieldType_DATE,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
		DateFormats:      []string{"yyyy-MM-dd HH:mm:SS"},
	}
	schemas = append(schemas, field1, field2, field3, field4, field5, field6)

	createSearchIndex(client, tableName, indexName, schemas)
}

func createSearchIndex(client *tablestore.TableStoreClient, tableName string, indexName string, fieldSchemas []*tablestore.FieldSchema) {
	fmt.Println("Begin to create table:", tableName)
	createTableRequest := new(tablestore.CreateTableRequest)

	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_STRING)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createTableRequest.TableMeta = tableMeta
	createTableRequest.TableOption = tableOption
	createTableRequest.ReservedThroughput = reservedThroughput

	_, err := client.CreateTable(createTableRequest)
	if err != nil {
		fmt.Println("Failed to create table with error:", err)
	} else {
		fmt.Println("Create table finished")
	}

	// create search index
	fmt.Println("Begin to create index:", indexName)
	request := &tablestore.CreateSearchIndexRequest{}
	request.TableName = tableName // 设置表名
	request.IndexName = indexName // 设置索引名
	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: fieldSchemas,
	}

	resp, err := client.CreateSearchIndex(request) // 调用client创建SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
}

func ListSearchIndex(client *tablestore.TableStoreClient, tableName string) {
	request := &tablestore.ListSearchIndexRequest{}
	request.TableName = tableName
	resp, err := client.ListSearchIndex(request)
	if err != nil {
		fmt.Println("error: ", err)
		return
	}
	for _, info := range resp.IndexInfo {
		fmt.Printf("%#v\n", info)
	}
	fmt.Println("ListSearchIndex finished, requestId: ", resp.ResponseInfo.RequestId)
}

func DescribeSearchIndex(client *tablestore.TableStoreClient, tableName string, indexName string) {
	request := &tablestore.DescribeSearchIndexRequest{}
	request.TableName = tableName
	request.IndexName = indexName
	resp, err := client.DescribeSearchIndex(request)
	if err != nil {
		fmt.Println("error: ", err)
		return
	}
	fmt.Println("FieldSchemas:")
	for _, schema := range resp.Schema.FieldSchemas {
		fmt.Printf("%s\n", schema)
	}
	if resp.Schema.IndexSort != nil {
		fmt.Printf("IndexSort:\n")
		for _, sorter := range resp.Schema.IndexSort.Sorters {
			fmt.Printf("\t%#v\n", sorter)
		}
	}
	fmt.Println("DescribeSearchIndex finished, requestId: ", resp.ResponseInfo.RequestId)
}

func DeleteSearchIndex(client *tablestore.TableStoreClient, tableName string, indexName string) {
	request := &tablestore.DeleteSearchIndexRequest{}
	request.TableName = tableName
	request.IndexName = indexName
	resp, err := client.DeleteSearchIndex(request)
	if err != nil {
		fmt.Println("error: ", err)
		return
	}
	fmt.Println("DeleteSearchIndex finished, requestId: ", resp.ResponseInfo.RequestId)
}

func WriteData(client *tablestore.TableStoreClient, tableName string) {
	keywords := []string{"hangzhou", "tablestore", "ots"}
	for i := 0; i < 100; i++ {
		putRowRequest := new(tablestore.PutRowRequest)
		putRowChange := new(tablestore.PutRowChange)
		putRowChange.TableName = tableName
		putPk := new(tablestore.PrimaryKey)
		putPk.AddPrimaryKeyColumn("pk1", fmt.Sprintf("pk_%d", i))

		putRowChange.PrimaryKey = putPk
		putRowChange.AddColumn("Col_Keyword", keywords[i%len(keywords)])
		putRowChange.AddColumn("Col_Long", int64(i))
		putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		putRowRequest.PutRowChange = putRowChange
		_, err := client.PutRow(putRowRequest)

		if err != nil {
			fmt.Println("putrow failed with error:", err)
		}
	}
}

func WriteDateForSearchQuery(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("Begin to write data")

	for idx := 0; idx < 2000; idx++ {
		putPK := new(tablestore.PrimaryKey)
		putPK.AddPrimaryKeyColumn("pk1", strconv.Itoa(idx))
		putRowChange := new(tablestore.PutRowChange)
		putRowChange.TableName = tableName
		putRowChange.PrimaryKey = putPK
		putRowChange.AddColumn("Col_Keyword", strconv.Itoa(idx))
		putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		putRowRequest := new(tablestore.PutRowRequest)
		putRowRequest.PutRowChange = putRowChange
		if _, err := client.PutRow(putRowRequest); err != nil {
			fmt.Println("Put test data failed with err: ", err)
		}
	}
	time.Sleep(30 * time.Second)

	fmt.Println("Write data finished.")
}

// WriteDataForQueryHighlighting 为高亮查询测试插入数据
func WriteDataForQueryHighlighting(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("Begin to write data")
	texts := []string{"When the world is dark and dreary", "And the night is long and weary,", "Look up to the stars above,", "And find the light of hope and love."}

	for idx, text := range texts {
		putPK := new(tablestore.PrimaryKey)
		putPK.AddPrimaryKeyColumn("pk1", strconv.Itoa(idx))
		putRowChange := new(tablestore.PutRowChange)
		putRowChange.TableName = tableName
		putRowChange.PrimaryKey = putPK
		putRowChange.AddColumn("Col_Text", text)
		nestedData := fmt.Sprintf("[{\"Level1_Text\":\"%s\",\"Level1_Nested\":[{\"Level2_Text\":\"%s\"}]}]", text, text)
		putRowChange.AddColumn("Col_Nested", nestedData)
		putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		putRowRequest := new(tablestore.PutRowRequest)
		putRowRequest.PutRowChange = putRowChange
		if _, err := client.PutRow(putRowRequest); err != nil {
			fmt.Println("Put test data failed with err: ", err)
		}
	}
	time.Sleep(30 * time.Second)

	fmt.Println("Write data finished.")
}

func SearchQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	fmt.Println("Begin to run search query")

	{
		// search limit set to -1
		searchRequest := &tablestore.SearchRequest{}
		searchRequest.
			SetTableName(tableName).
			SetColumnsToGet(&tablestore.ColumnsToGet{ReturnAllFromIndex: true}).
			SetIndexName(indexName).
			SetSearchQuery(search.NewSearchQuery().
				SetLimit(-1))
		if resp, err := client.Search(searchRequest); err != nil {
			fmt.Println("search query failed with err: ", err)
		} else {
			fmt.Println("RequestId: " + resp.RequestId)
			fmt.Printf("except: 1000, actual: %d\n", len(resp.SearchHits))
		}

		// search limit default
		searchRequest = &tablestore.SearchRequest{}
		searchRequest.
			SetTableName(tableName).
			SetColumnsToGet(&tablestore.ColumnsToGet{ReturnAllFromIndex: true}).
			SetIndexName(indexName).
			SetSearchQuery(search.NewSearchQuery())
		if resp, err := client.Search(searchRequest); err != nil {
			fmt.Println("search query failed with err: ", err)
		} else {
			fmt.Println("RequestId: " + resp.RequestId)
			fmt.Printf("except: 10, actual: %d\n", len(resp.SearchHits))
		}
	}

	fmt.Println("search query finished")
}

// QueryHighlightingSample 查询高亮示例
func QueryHighlightingSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	fmt.Println("Begin to run highlight query")
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.
		SetTableName(tableName).
		SetIndexName(indexName).
		SetSearchQuery(search.NewSearchQuery().
			SetLimit(5).
			SetQuery(&search.BoolQuery{
				ShouldQueries: []search.Query{
					&search.MatchQuery{
						FieldName: "Col_Text",
						Text:      "stars dark light",
					},
					&search.NestedQuery{
						Path:      "Col_Nested",
						ScoreMode: search.ScoreMode_Min,
						Query: &search.BoolQuery{
							ShouldQueries: []search.Query{
								&search.MatchQuery{
									FieldName: "Col_Nested.Level1_Text",
									Text:      "starts dark light",
								},
								&search.NestedQuery{
									Path:      "Col_Nested.Level1_Nested",
									ScoreMode: search.ScoreMode_Max,
									Query: &search.MatchQuery{
										FieldName: "Col_Nested.Level1_Nested.Level2_Text",
										Text:      "starts dark light",
									},
									InnerHits: &search.InnerHits{
										Offset: proto.Int32(0),
										Limit:  proto.Int32(3),
										Highlight: &search.Highlight{
											FieldHighlightParameters: map[string]*search.HighlightParameter{
												"Col_Nested.Level1_Nested.Level2_Text": {
													NumberOfFragments: proto.Int32(5),
												},
											},
										},
									},
								},
							},
						},
						InnerHits: &search.InnerHits{
							Offset: proto.Int32(0),
							Limit:  proto.Int32(3),
							Sort: &search.Sort{
								Sorters: []search.Sorter{
									&search.ScoreSort{
										Order: search.SortOrder_ASC.Enum(),
									},
								},
							},
							Highlight: &search.Highlight{
								HighlightEncoder: search.PlainMode.Enum(),
								FieldHighlightParameters: map[string]*search.HighlightParameter{
									"Col_Nested.Level1_Text": {
										NumberOfFragments: proto.Int32(5),
										FragmentSize:      proto.Int32(100),
										PreTag:            proto.String("<b>"),
										PostTag:           proto.String("</b>"),
									},
								},
							},
						},
					},
				},
			}).
			SetHighlight(search.NewHighlight().
				SetHighlightEncoder(search.PlainMode).
				AddFieldHighlightParameter("Col_Text", search.NewHighlightParameter().
					SetPreTag("<em>").
					SetPostTag("</em>"))).
			SetGetTotalCount(false)).
		SetColumnsToGet(&tablestore.ColumnsToGet{ReturnAllFromIndex: true})
	if resp, err := client.Search(searchRequest); err != nil {
		fmt.Println("Highlighting query failed with err: ", err)
	} else {
		fmt.Println("RequestId: " + resp.RequestId)
		printSearchHit(resp.SearchHits, "  ")
	}
	fmt.Println("highlight query finished")
}

func printSearchHit(searchHits []*tablestore.SearchHit, padding string) {
	for _, searchHit := range searchHits {
		if searchHit.Score != nil {
			fmt.Printf("%sScore: %f\n", padding, *searchHit.Score)
		}

		if searchHit.NestedDocOffset != nil {
			fmt.Printf("%sOffset: %d\n", padding, *searchHit.NestedDocOffset)
		}

		if searchHit.Row != nil {
			fmt.Printf("%sRow: %v\n", padding, *searchHit.Row)
		}

		if searchHit.HighlightResultItem != nil && len(searchHit.HighlightResultItem.HighlightFields) != 0 {
			fmt.Printf("%sHighlight: \n", padding)
			for colName, highlightResult := range searchHit.HighlightResultItem.HighlightFields {
				fmt.Printf("%sColumnName: %s, Highlight_Fragments: %v\n", padding+padding, colName, highlightResult.Fragments)
			}
		}

		if searchHit.SearchInnerHits != nil && len(searchHit.SearchInnerHits) != 0 {
			fmt.Printf("%sInnerHits: \n", padding)
			for path, innerSearchHit := range searchHit.SearchInnerHits {
				fmt.Printf("%sPath: %s\n", padding+padding, path)
				fmt.Printf("%sSearchHit: \n", padding+padding)
				printSearchHit(innerSearchHit.SearchHits, padding+padding)
			}
		}

		fmt.Println("")
	}
}

/**
 * 为Aggregation和GroupBy测试插入数据
 */
func WriteDataForAggregationAndGroupBy(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("Begin to write data")
	keywords := []string{"hangzhou", "tablestore", "ots"}
	keywords2 := []string{"red", "blue"}
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

	for i := 0; i < 10; i++ {
		putRowRequest := new(tablestore.PutRowRequest)
		putRowChange := new(tablestore.PutRowChange)
		putRowChange.TableName = tableName
		putPk := new(tablestore.PrimaryKey)
		putPk.AddPrimaryKeyColumn("pk1", fmt.Sprintf("pk_%d", i))

		putRowChange.PrimaryKey = putPk
		putRowChange.AddColumn("Col_Keyword", keywords[i%len(keywords)])
		putRowChange.AddColumn("Col_Keyword2", keywords2[i%len(keywords2)])
		if i != 0 {
			putRowChange.AddColumn("Col_Long", int64(i))
		}
		if i != 9 {
			putRowChange.AddColumn("Col_Double", float64(i))
		}
		putRowChange.AddColumn("Col_Date", time.Now().AddDate(0, 0, i).Format("2006-01-02 15:04:05"))
		putRowChange.AddColumn("Col_GeoPoint", geopoints[i])
		putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		putRowRequest.PutRowChange = putRowChange
		_, err := client.PutRow(putRowRequest)

		if err != nil {
			fmt.Println("putrow failed with error:", err)
		}
	}

	time.Sleep(20 * time.Second)
}

/**
 * 使用Token进行翻页读取。
 * 如果SearchResponse返回了NextToken，可以使用这个Token发起下一次查询，
 * 直到NextToken为空(nil)，此时代表所有符合条件的数据已经读完。
 */
func QueryRowsWithToken(client *tablestore.TableStoreClient, tableName string, indexName string) {
	querys := []search.Query{
		&search.MatchAllQuery{},
		&search.TermQuery{
			FieldName: "Col_Keyword",
			Term:      "tablestore",
		},
	}
	for _, query := range querys {
		fmt.Printf("Test query: %#v\n", query)
		searchRequest := &tablestore.SearchRequest{}
		searchRequest.SetTableName(tableName)
		searchRequest.SetIndexName(indexName)
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchQuery.SetLimit(10)
		searchQuery.SetGetTotalCount(true)
		searchRequest.SetSearchQuery(searchQuery)
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		rows := searchResponse.Rows
		requestCount := 1
		for searchResponse.NextToken != nil {
			searchQuery.SetToken(searchResponse.NextToken)
			searchResponse, err = client.Search(searchRequest)
			if err != nil {
				fmt.Printf("%#v", err)
				return
			}
			requestCount++
			for _, r := range searchResponse.Rows {
				rows = append(rows, r)
			}
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
		fmt.Println("TotalCount: ", searchResponse.TotalCount)
		fmt.Println("RowsSize: ", len(rows))
		fmt.Println("RequestCount: ", requestCount)
	}
}

func MatchAllQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.MatchAllQuery{}
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetLimit(0)
	searchQuery.SetGetTotalCount(true) // 设置GetTotalCount为true后才会返回总条数
	searchRequest.SetSearchQuery(searchQuery)
	searchRequest.SetTimeoutMs(30000) //可以显示设置请求超时时间
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("TotalCount: ", searchResponse.TotalCount)
}

func FieldSort_missingField(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.MatchAllQuery{}
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetSort(&search.Sort{
		Sorters: []search.Sorter{
			&search.FieldSort{
				FieldName:    "Col_Long",
				Order:        search.SortOrder_ASC.Enum(),
				MissingField: proto.String("Col_Long_Sec"), //如果排序字段Col_Long缺失的时候用Col_Long_Sec替换
				MissingValue: 50,                           // 如果排序字段及替换字段都缺失情况下用missingValue替换
				//MissingValue: search.FirstWhenMissing, // 如果missingValue设置为FirstWhenMissing，当排序字段值缺省时候排在最前面
			},
		},
	})
	searchQuery.SetLimit(10)
	searchRequest.SetSearchQuery(searchQuery)
	searchRequest.SetTimeoutMs(30000) //可以显示设置请求超时时间
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 *  查询表中Col_Keyword这一列的值能够匹配"hangzhou"的数据，返回匹配到的总行数和一些匹配成功的行。
 */
func MatchQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.MatchQuery{}   // 设置查询类型为MatchQuery
	query.FieldName = "Col_Keyword" // 设置要匹配的字段
	query.Text = "hangzhou"         // 设置要匹配的值
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetOffset(0) // 设置offset为0
	searchQuery.SetLimit(20) // 设置limit为20，表示最多返回20条数据
	searchRequest.SetSearchQuery(searchQuery)
	searchResponse, err := client.Search(searchRequest)
	if err != nil { // 判断异常
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount)     // 匹配的总行数
	fmt.Println("RowCount: ", len(searchResponse.Rows))        // 返回的行数
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody)) // 不设置columnsToGet，默认只返回主键
	}
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 查询表中Col_Text这一列的值能够匹配"hangzhou shanghai"的数据，匹配条件为短语匹配(要求短语完整的按照顺序匹配)，返回匹配到的总行数和一些匹配成功的行。
 */
func MatchPhraseQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.MatchPhraseQuery{} // 设置查询类型为MatchPhraseQuery
	query.FieldName = "Col_Text"        // 设置要匹配的字段
	query.Text = "hangzhou shanghai"    // 设置要匹配的值
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetOffset(0) // 设置offset为0
	searchQuery.SetLimit(20) // 设置limit为20，表示最多返回20条数据
	searchRequest.SetSearchQuery(searchQuery)
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 查询表中Col_Keyword这一列精确匹配"hangzhou"的数据。
 */
func TermQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.TermQuery{}    // 设置查询类型为TermQuery
	query.FieldName = "Col_Keyword" // 设置要匹配的字段
	query.Term = "hangzhou"         // 设置要匹配的值
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetLimit(100)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 查询表中Col_Keyword这一列精确匹配"hangzhou"或"tablestore"的数据。
 */
func TermsQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.TermsQuery{}   // 设置查询类型为TermQuery
	query.FieldName = "Col_Keyword" // 设置要匹配的字段
	terms := make([]interface{}, 0)
	terms = append(terms, "hangzhou")
	terms = append(terms, "tablestore")
	query.Terms = terms // 设置要匹配的值
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetLimit(100)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 查询表中Col_Keyword这一列前缀为"hangzhou"的数据。
 */
func PrefixQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.PrefixQuery{}  // 设置查询类型为PrefixQuery
	query.FieldName = "Col_Keyword" // 设置要匹配的字段
	query.Prefix = "hangzhou"       // 设置前缀
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 使用通配符查询，查询表中Col_Keyword这一列的值匹配"hang*u"的数据
 */
func WildcardQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.WildcardQuery{} // 设置查询类型为WildcardQuery
	query.FieldName = "Col_Keyword"
	query.Value = "hang*u"
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 查询表中Col_Long这一列大于3的数据，结果按照Col_Long这一列的值逆序排序。
 */
func RangeQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	searchQuery := search.NewSearchQuery()
	rangeQuery := &search.RangeQuery{} // 设置查询类型为RangeQuery
	rangeQuery.FieldName = "Col_Long"  // 设置针对哪个字段
	rangeQuery.GT(3)                   // 设置该字段的范围条件，大于3
	searchQuery.SetQuery(rangeQuery)
	// 设置按照Col_Long这一列逆序排序
	searchQuery.SetSort(&search.Sort{
		[]search.Sorter{
			&search.FieldSort{
				FieldName: "Col_Long",
				Order:     search.SortOrder_DESC.Enum(),
			},
		},
	})
	searchRequest.SetSearchQuery(searchQuery)
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * Col_GeoPoint是GeoPoint类型，查询表中Col_GeoPoint这一列的值在左上角为"10,0", 右下角为"0,10"的矩形范围内的数据。
 */
func GeoBoundingBoxQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.GeoBoundingBoxQuery{} // 设置查询类型为GeoBoundingBoxQuery
	query.FieldName = "Col_GeoPoint"       // 设置比较哪个字段的值
	query.TopLeft = "10,0"                 // 设置矩形左上角
	query.BottomRight = "0,10"             // 设置矩形右下角
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 查询表中Col_GeoPoint这一列的值距离中心点不超过一定距离的数据。
 */
func GeoDistanceQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.GeoDistanceQuery{} // 设置查询类型为GeoDistanceQuery
	query.FieldName = "Col_GeoPoint"
	query.CenterPoint = "5,5"       // 设置中心点
	query.DistanceInMeter = 10000.0 // 设置到中心点的距离条件，不超过10000米
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 查询表中Col_GeoPoint这一列的值在一个给定多边形范围内的数据。
 */
func GeoPolygonQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.GeoPolygonQuery{} // 设置查询类型为GeoDistanceQuery
	query.FieldName = "Col_GeoPoint"
	query.Points = []string{"0,0", "5,5", "5,0"} // 设置多边形的顶点
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}

/**
 * 通过BoolQuery进行复合条件查询。
 */
func BoolQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)

	/**
	 * 查询条件一：RangeQuery，Col_Long这一列的值要大于3
	 */
	rangeQuery := &search.RangeQuery{}
	rangeQuery.FieldName = "Col_Long"
	rangeQuery.GT(3)

	/**
	 * 查询条件二：MatchQuery，Col_Keyword这一列的值要匹配"hangzhou"
	 */
	matchQuery := &search.MatchQuery{}
	matchQuery.FieldName = "Col_Keyword"
	matchQuery.Text = "hangzhou"

	{
		/**
		 * 构造一个BoolQuery，设置查询条件是必须同时满足"条件一"和"条件二"
		 */
		boolQuery := &search.BoolQuery{
			MustQueries: []search.Query{
				rangeQuery,
				matchQuery,
			},
		}
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(boolQuery)
		searchRequest.SetSearchQuery(searchQuery)
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
		fmt.Println("RowCount: ", len(searchResponse.Rows))
	}
	{
		/**
		 * 构造一个BoolQuery，设置查询条件是至少满足"条件一"和"条件二"中的一个
		 */
		boolQuery := &search.BoolQuery{
			ShouldQueries: []search.Query{
				rangeQuery,
				matchQuery,
			},
			MinimumShouldMatch: proto.Int32(1),
		}
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(boolQuery)
		searchRequest.SetSearchQuery(searchQuery)
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
		fmt.Println("RowCount: ", len(searchResponse.Rows))
	}
}

/**
 * 创建一个SearchIndex，为TEXT类型索引列自定义分词器
 */
func Analysis(client *tablestore.TableStoreClient, tableName string, indexName string) {
	fmt.Println("Begin to create table:", tableName)
	createtableRequest := new(tablestore.CreateTableRequest)

	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_STRING)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput

	_, err := client.CreateTable(createtableRequest)
	if err != nil {
		fmt.Println("Failed to create table with error:", err)
	} else {
		fmt.Println("Create table finished")
	}

	fmt.Println("Begin to create index:", indexName)
	request := &tablestore.CreateSearchIndexRequest{}
	request.TableName = tableName // 设置表名
	request.IndexName = indexName // 设置索引名

	schemas := []*tablestore.FieldSchema{}

	analyzer1 := tablestore.Analyzer_SingleWord
	analyzerParam1 := tablestore.SingleWordAnalyzerParameter{
		CaseSensitive: proto.Bool(true),
		DelimitWord:   proto.Bool(true),
	}
	field1 := &tablestore.FieldSchema{
		FieldName:         proto.String("Col_SingleWord"), // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:         tablestore.FieldType_TEXT,      // 设置字段类型
		Index:             proto.Bool(true),               // 设置开启索引
		Analyzer:          &analyzer1,                     // 设置分词器
		AnalyzerParameter: analyzerParam1,                 // 设置分词器参数(可选)
	}

	analyzer2 := tablestore.Analyzer_MaxWord
	field2 := &tablestore.FieldSchema{
		FieldName: proto.String("Col_MaxWord"), // 设置字段名，使用proto.String用于获取字符串指针
		FieldType: tablestore.FieldType_TEXT,   // 设置字段类型
		Index:     proto.Bool(true),            // 设置开启索引
		Analyzer:  &analyzer2,                  // 设置分词器
	}

	analyzer3 := tablestore.Analyzer_MinWord
	field3 := &tablestore.FieldSchema{
		FieldName: proto.String("Col_MinWord"), // 设置字段名，使用proto.String用于获取字符串指针
		FieldType: tablestore.FieldType_TEXT,   // 设置字段类型
		Index:     proto.Bool(true),            // 设置开启索引
		Analyzer:  &analyzer3,                  // 设置分词器
	}

	analyzer4 := tablestore.Analyzer_Split
	analyzerParam4 := tablestore.SplitAnalyzerParameter{Delimiter: proto.String("-")}
	field4 := &tablestore.FieldSchema{
		FieldName:         proto.String("Col_Split"), // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:         tablestore.FieldType_TEXT, // 设置字段类型
		Index:             proto.Bool(true),          // 设置开启索引
		Analyzer:          &analyzer4,                // 设置分词器
		AnalyzerParameter: analyzerParam4,            // 设置分词器参数(可选)
	}

	analyzer5 := tablestore.Analyzer_Fuzzy
	analyzerParam5 := tablestore.FuzzyAnalyzerParameter{
		MinChars: 1,
		MaxChars: 4,
	}
	field5 := &tablestore.FieldSchema{
		FieldName:         proto.String("Col_Fuzzy"), // 设置字段名，使用proto.String用于获取字符串指针
		FieldType:         tablestore.FieldType_TEXT, // 设置字段类型
		Index:             proto.Bool(true),          // 设置开启索引
		Analyzer:          &analyzer5,                // 设置分词器
		AnalyzerParameter: analyzerParam5,            // 设置分词器参数(可选)
	}

	schemas = append(schemas, field1, field2, field3, field4, field5)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // 设置SearchIndex包含的字段
	}
	resp, err := client.CreateSearchIndex(request) // 调用client创建SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)

	// write data
	putRowRequest := new(tablestore.PutRowRequest)
	putRowChange := new(tablestore.PutRowChange)
	putRowChange.TableName = tableName
	putPk := new(tablestore.PrimaryKey)
	putPk.AddPrimaryKeyColumn("pk1", "pk1_value")

	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("Col_SingleWord", "中华人民共和国国歌 People's Republic of China")
	putRowChange.AddColumn("Col_MaxWord", "中华人民共和国国歌 People's Republic of China")
	putRowChange.AddColumn("Col_MinWord", "中华人民共和国国歌 People's Republic of China")
	putRowChange.AddColumn("Col_Split", "2019-05-01")
	putRowChange.AddColumn("Col_Fuzzy", "老王是个工程师")
	putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, err2 := client.PutRow(putRowRequest)

	if err2 != nil {
		fmt.Println("putrow failed with error:", err2)
	}

	// wait a while
	time.Sleep(time.Duration(30) * time.Second)

	// search
	{
		searchRequest := &tablestore.SearchRequest{}
		searchRequest.SetTableName(tableName)
		searchRequest.SetIndexName(indexName)
		query := &search.MatchQuery{}      // 设置查询类型为MatchQuery
		query.FieldName = "Col_SingleWord" // 设置要匹配的字段
		query.Text = "歌"                   // 设置要匹配的值
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchRequest.SetSearchQuery(searchQuery)

		// 设置返回所有列
		searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
			ReturnAll: true,
		})
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
		fmt.Println("RowCount: ", len(searchResponse.Rows))
		for _, row := range searchResponse.Rows {
			jsonBody, err := json.Marshal(row)
			if err != nil {
				panic(err)
			}
			fmt.Println("Row: ", string(jsonBody))
		}
	}

	{
		searchRequest := &tablestore.SearchRequest{}
		searchRequest.SetTableName(tableName)
		searchRequest.SetIndexName(indexName)
		query := &search.MatchQuery{}   // 设置查询类型为MatchQuery
		query.FieldName = "Col_MaxWord" // 设置要匹配的字段
		query.Text = "中华人民共和国"          // 设置要匹配的值
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchRequest.SetSearchQuery(searchQuery)

		// 设置返回所有列
		searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
			ReturnAll: true,
		})
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
		fmt.Println("RowCount: ", len(searchResponse.Rows))
		for _, row := range searchResponse.Rows {
			jsonBody, err := json.Marshal(row)
			if err != nil {
				panic(err)
			}
			fmt.Println("Row: ", string(jsonBody))
		}
	}

	{
		searchRequest := &tablestore.SearchRequest{}
		searchRequest.SetTableName(tableName)
		searchRequest.SetIndexName(indexName)
		query := &search.MatchQuery{} // 设置查询类型为MatchQuery
		query.FieldName = "Col_Split" // 设置要匹配的字段
		query.Text = "2019"           // 设置要匹配的值
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchRequest.SetSearchQuery(searchQuery)

		// 设置返回所有列
		searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
			ReturnAll: true,
		})
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
		fmt.Println("RowCount: ", len(searchResponse.Rows))
		for _, row := range searchResponse.Rows {
			jsonBody, err := json.Marshal(row)
			if err != nil {
				panic(err)
			}
			fmt.Println("Row: ", string(jsonBody))
		}
	}

	{
		searchRequest := &tablestore.SearchRequest{}
		searchRequest.SetTableName(tableName)
		searchRequest.SetIndexName(indexName)
		query := &search.MatchQuery{} // 设置查询类型为MatchQuery
		query.FieldName = "Col_Fuzzy" // 设置要匹配的字段
		query.Text = "程"              // 设置要匹配的值
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchRequest.SetSearchQuery(searchQuery)

		// 设置返回所有列
		searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
			ReturnAll: true,
		})
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
		fmt.Println("RowCount: ", len(searchResponse.Rows))
		for _, row := range searchResponse.Rows {
			jsonBody, err := json.Marshal(row)
			if err != nil {
				panic(err)
			}
			fmt.Println("Row: ", string(jsonBody))
		}
	}
}

/**
 * Aggregation示例
 */
func AggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	var percentiles = make([]float64, 3)
	percentiles[0] = 0.0
	percentiles[1] = 50.0
	percentiles[2] = 100.0

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                   //匹配所有行
			SetLimit(100).                                                       //限制返回前100行结果
			Aggregation(search.NewAvgAggregation("agg1", "Col_Long")).           //计算Col_Long字段的平均值
			Aggregation(search.NewDistinctCountAggregation("agg2", "Col_Long")). //计算Col_Long字段不同取值的个数
			Aggregation(search.NewMaxAggregation("agg3", "Col_Long")).           //计算Col_Long字段的最大值
			Aggregation(search.NewSumAggregation("agg4", "Col_Long")).           //计算Col_Long字段的和
			Aggregation(search.NewCountAggregation("agg5", "Col_Long")).         //计算存在Col_Long字段的行数
			Aggregation(search.NewTopRowsAggregation("agg6").SetLimit(1).SetSort(&search.Sort{
				Sorters: []search.Sorter{
					&search.FieldSort{
						FieldName: "Col_Long",
						Order:     search.SortOrder_DESC.Enum(),
					},
				},
			})).
			Aggregation(search.NewPercentilesAggregation("agg7", "Col_Long").SetMissing(10).SetPercents(percentiles)))

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:          false,
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	aggResults := searchResponse.AggregationResults //获取所有统计结果

	//avg agg
	agg1, err := aggResults.Avg("agg1") //获取名字为"agg1"的Aggregation结果，类型为Avg
	if err != nil {
		panic(err)
	}
	if agg1.HasValue() { //名字为"agg1"的Aggregation结果 是否Value值
		fmt.Println("(avg) agg1: ", agg1.Value) //打印Col_Long字段平均值
	} else {
		fmt.Println("(avg) agg1: no value") //所有行都不存在Col_Long字段
	}

	//distinct count agg
	agg2, err := aggResults.DistinctCount("agg2") //获取名字为"agg2"的Aggregation结果，类型为DistinctCount
	if err != nil {
		panic(err)
	}
	fmt.Println("(distinct) agg2: ", agg2.Value) //打印Col_Long字段不同取值的个数

	//max agg
	agg3, err := aggResults.Max("agg3") //获取名字为"agg3"的Aggregation结果，类型为Max
	if err != nil {
		panic(err)
	}
	if agg3.HasValue() {
		fmt.Println("(max) agg3: ", agg3.Value) //打印Col_Long字段最大值
	} else {
		fmt.Println("(max) agg3: no value") //所有行都不存在Col_Long字段
	}

	//sum agg
	agg4, err := aggResults.Sum("agg4") //获取名字为"agg4"的Aggregation结果，类型为Sum
	if err != nil {
		panic(err)
	}
	fmt.Println("(sum) agg4: ", agg4.Value) //打印Col_Long字段的和

	//count agg
	agg5, err := aggResults.Count("agg5") //获取名字为"agg5"的Aggregation结果，类型为Count
	if err != nil {
		panic(err)
	}
	fmt.Println("(count) agg5: ", agg5.Value) //打印存在Col_Long字段的个数

	//topRows agg
	agg6, err := aggResults.TopRows("agg6") //获取名字为"agg6"的Aggregation结果，类型为TopRows
	if err != nil {
		panic(err)
	}
	jsonBody, err := json.Marshal(agg6.Value)
	if err != nil {
		panic(err)
	}
	fmt.Println("TowRow: ", string(jsonBody)) //打印返回的row

	//percentiles agg
	agg7, err := aggResults.Percentiles("agg7") //获取名字为"agg7"的Aggregation结果，类型为Percentiles
	if err != nil {
		panic(err)
	}
	for _, item := range agg7.PercentilesAggregationItems {
		fmt.Println("\t(percentiles)key: ", item.Key, ", value: ", item.Value.Value) //打印返回的value
	}
}

func AvgAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                            //匹配所有行
			SetLimit(100).                                                //限制返回前100行结果
			Aggregation(search.NewAvgAggregation("avg_agg", "Col_Long"))) //计算Col_Long字段的平均值
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:          false,
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	aggResults := searchResponse.AggregationResults //获取所有统计结果

	//avg agg
	aggregationResult, err := aggResults.Avg("avg_agg") //获取名字为"avg_agg"的Aggregation结果，类型为Avg
	if err != nil {
		panic(err)
	}
	if aggregationResult.HasValue() {
		fmt.Println("avg_agg: ", aggregationResult.Value) //打印Col_Long字段平均值
	} else {
		fmt.Println("avg_agg: no value") //所有行都不存在Col_Long字段
	}
}

func DistinctAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                                 //匹配所有行
			SetLimit(100).                                                                     //限制返回前100行结果
			Aggregation(search.NewDistinctCountAggregation("distinct_count_agg", "Col_Long"))) //计算Col_Long字段不同取值的个数
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:          false,
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	aggResults := searchResponse.AggregationResults //获取所有统计结果

	aggregationResult, err := aggResults.DistinctCount("distinct_count_agg") //获取名字为"distinct_count_agg"的Aggregation结果，类型为DistinctCount
	if err != nil {
		panic(err)
	}
	fmt.Println("distinct_count_agg: ", aggregationResult.Value) //打印Col_Long字段不同取值的个数
}

func MaxAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                            //匹配所有行
			SetLimit(100).                                                //限制返回前100行结果
			Aggregation(search.NewMaxAggregation("max_agg", "Col_Long"))) //计算Col_Long字段的最大值
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:          false,
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	aggResults := searchResponse.AggregationResults //获取所有统计结果

	aggregationResult, err := aggResults.Max("max_agg") //获取名字为"max_agg"的Aggregation结果，类型为Max
	if err != nil {
		panic(err)
	}
	if aggregationResult.HasValue() {
		fmt.Println("max_agg: ", aggregationResult.Value) //打印Col_Long字段最大值
	} else {
		fmt.Println("max_agg: no value") //所有行都不存在Col_Long字段
	}
}

func SumAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                            //匹配所有行
			SetLimit(100).                                                //限制返回前100行结果
			Aggregation(search.NewSumAggregation("sum_agg", "Col_Long"))) //计算Col_Long字段的和
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:          false,
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	aggResults := searchResponse.AggregationResults //获取所有统计结果

	aggregationResult, err := aggResults.Sum("sum_agg") //获取名字为"sum_agg"的Aggregation结果，类型为Sum
	if err != nil {
		panic(err)
	}
	fmt.Println("sum_agg: ", aggregationResult.Value) //打印Col_Long字段的和
}

func CountAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                //匹配所有行
			SetLimit(100).                                                    //限制返回前100行结果
			Aggregation(search.NewCountAggregation("count_agg", "Col_Long"))) //计算存在Col_Long字段的行数
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:          false,
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	aggResults := searchResponse.AggregationResults //获取所有统计结果

	aggregationResult, err := aggResults.Count("count_agg") //获取名字为"count_agg"的Aggregation结果，类型为Count
	if err != nil {
		panic(err)
	}
	fmt.Println("count_agg: ", aggregationResult.Value) //打印存在Col_Long字段的个数
}

func TopRowsAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). //匹配所有行
			SetLimit(100).                     //限制返回前100行结果
			Aggregation(search.NewTopRowsAggregation("top_rows_agg").SetLimit(1).SetSort(&search.Sort{
				Sorters: []search.Sorter{
					&search.FieldSort{
						FieldName: "Col_Long",
						Order:     search.SortOrder_DESC.Enum(),
					},
				},
			})))
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:          false,
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	aggResults := searchResponse.AggregationResults //获取所有统计结果

	aggregationResult, err := aggResults.TopRows("top_rows_agg") //获取名字为"top_rows_agg"的Aggregation结果，类型为TopRows
	if err != nil {
		panic(err)
	}
	jsonBody, err := json.Marshal(aggregationResult.Value)
	if err != nil {
		panic(err)
	}
	fmt.Println("top_rows_agg: ", string(jsonBody)) //打印返回的row
}

func PercentilesAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	var percentiles = make([]float64, 3)
	percentiles[0] = 0.0
	percentiles[1] = 50.0
	percentiles[2] = 100.0

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). //匹配所有行
			SetLimit(100).                     //限制返回前100行结果
			Aggregation(search.NewPercentilesAggregation("percentiles_agg", "Col_Long").SetMissing(10).SetPercents(percentiles)))

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:          false,
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	aggResults := searchResponse.AggregationResults //获取所有统计结果

	aggregationResult, err := aggResults.Percentiles("percentiles_agg") //获取名字为"percentiles_agg"的Aggregation结果，类型为Percentiles
	if err != nil {
		panic(err)
	}
	for _, item := range aggregationResult.PercentilesAggregationItems {
		fmt.Println("\tkey: ", item.Key, ", value: ", item.Value.Value) //打印返回的value
	}
}

/**
 * GroupBy示例
 */
func GroupBySample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                       //匹配所有行
			SetLimit(100).                                           //限制返回前100行结果
			GroupBy(search.NewGroupByField("group1", "Col_Keyword"). //对Col_Keyword字段做GroupByField取值聚合
											GroupBySorters([]search.GroupBySorter{}).                          //可以指定返回结果分桶的顺序
											Size(2).                                                           //仅返回前2个分桶
											SubAggregation(search.NewAvgAggregation("sub_agg1", "Col_Long")).  //对每个分桶进行子统计(Aggregation)
											SubGroupBy(search.NewGroupByField("sub_group1", "Col_Keyword2"))). //对每个分桶进行子聚合(GroupBy)
			GroupBy(search.NewGroupByRange("group2", "Col_Long").    //对Col_Long字段做GroupByRange范围
											Range(search.NegInf, 3). //第一个分桶包含Col_Long在(-∞, 3)的索引行
											Range(3, 5).             //第二个分桶包含Col_Long在[3, 5)的索引行
											Range(5, search.Inf)).   //第三个分桶包含Col_Long在[5, +∞)的索引行
			GroupBy(search.NewGroupByFilter("group3").               //做GroupByFilter过滤聚合
											Query(&search.TermQuery{ //第一个分桶包含Col_Keyword字段取值为"hangzhou"的索引行
					FieldName: "Col_Keyword",
					Term:      "hangzhou",
				}).
				Query(&search.RangeQuery{ //第二个分桶包含Col_Long字段取值在[3, 5]范围的索引行
																			FieldName:    "Col_Long",
																			From:         3,
																			To:           5,
																			IncludeLower: true,
																			IncludeUpper: true})).
			GroupBy(search.NewGroupByGeoDistance("group4", "Col_GeoPoint", search.GeoPoint{Lat: 30.137817, Lon: 120.08681}). //对Col_GeoPoint字段做GroupByGeoDistance地理范围聚合
																		Range(search.NegInf, 10000). //第一个分桶包含Col_GeoPoint离中心点距离(-∞, 10km)的索引行
																		Range(10000, 15000).         //第二个分桶包含Col_GeoPoint离中心点距离(10km, 15km)的索引行
																		Range(15000, search.Inf)).   //第三个分桶包含Col_GeoPoint离中心点距离(15km, +∞)的索引行
			GroupBy(search.NewGroupByHistogram("group5", "Col_Long").
											SetInterval(10).
											SetMinDocCount(1).
											SetFiledRange(0, 100).
											SetMissing(3)).
			GroupBy(search.NewGroupByDateHistogram("group6", "Col_date"). // Suppose date format is : 'yyyy-MM-dd HH:mm:ss'
											SetInterval(model.DateTimeValue{Unit: model.DateTimeUnit_HOUR.Enum(), Value: proto.Int32(30)}).
											SetMinDocCount(1).
											SetFiledRange("2022-01-01 12:13:14", "2022-01-05 12:13:14").
											SetMissing("2022-01-06 12:13:14")).
			GroupBy(search.NewGroupByGeoGrid("group7", "Col_geo").
				SetPrecision(model.GHP_156KM_156KM_3).
				SetSize(10)))

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	groupByResults := searchResponse.GroupByResults //获取所有聚合结果

	group1, err := groupByResults.GroupByField("group1") //获取名字为"group1"的GroupBy结果，类型为GroupByField
	if err != nil {
		panic(err)
	}
	fmt.Println("group1: ")
	for _, item := range group1.Items { //遍历返回的所有分桶
		//item
		fmt.Println("\tkey: ", item.Key, ", rowCount: ", item.RowCount) //打印本次分桶的行数

		//sub agg
		subAgg1, err := item.SubAggregations.Avg("sub_agg1") //获取名字为sub_agg1的子统计的结果
		if err != nil {
			panic(err)
		}
		if subAgg1.HasValue() { //如果子统计sub_agg1计算出了Col_Long字段的平均值，则HasValue()返回true
			fmt.Println("\t\tsub_agg1: ", subAgg1.Value) //打印本次分桶中，子统计计算出来的Col_Long字段的平均值
		}

		//sub group by
		subGroup1, err := item.SubGroupBys.GroupByField("sub_group1") //获取名字为sub_group1的子聚合的结果
		if err != nil {
			panic(err)
		}
		fmt.Println("\t\tsub_group1")
		for _, subItem := range subGroup1.Items { //遍历名字为sub_group1的子聚合结果
			fmt.Println("\t\t\tkey: ", subItem.Key, ", rowCount: ", subItem.RowCount) //打印sub_group1子聚合的结果分桶，即分桶中的行数
			tablestore.Assert(subItem.SubAggregations.Empty(), "")
			tablestore.Assert(subItem.SubGroupBys.Empty(), "")
		}
	}

	//group by range
	group2, err := groupByResults.GroupByRange("group2") //获取名字为"group2"的GroupBy结果，类型为GroupByRange
	if err != nil {
		panic(err)
	}
	fmt.Println("group2: ")
	for _, item := range group2.Items { //遍历返回的所有分桶
		fmt.Println("\t[", item.From, ", ", item.To, "), rowCount: ", item.RowCount) //打印本次分桶的行数
	}

	//group by filter
	group3, err := groupByResults.GroupByFilter("group3") //获取名字为"group3"的GroupBy结果，类型为GroupByFilter
	if err != nil {
		panic(err)
	}
	fmt.Println("group3: ")
	for _, item := range group3.Items { //遍历返回的所有分桶
		fmt.Println("\trowCount: ", item.RowCount) //打印本次分桶的行数
	}

	//group by geo distance
	group4, err := groupByResults.GroupByGeoDistance("group4") //获取名字为"group4"的GroupBy结果，类型为GroupByGeoDistance
	if err != nil {
		panic(err)
	}
	fmt.Println("group4: ")
	for _, item := range group4.Items { //遍历返回的所有分桶
		fmt.Println("\t[", item.From, ", ", item.To, "), rowCount: ", item.RowCount) //打印本次分桶的行数
	}

	//group by histogram
	group5, err := groupByResults.GroupByHistogram("group5") //获取名字为"group5"的GroupBy结果，类型为GroupByHistogram
	if err != nil {
		panic(err)
	}
	fmt.Println("group5: ")
	for _, item := range group5.Items {
		fmt.Println("key: ", item.Key.Value, ", value: ", item.Value) //打印返回的value
	}

	// group by date histogram
	searchRequest = &tablestore.SearchRequest{}
	searchRequest.
		SetTableName(tableName).
		SetIndexName(indexName).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).
			SetLimit(100).
			GroupBy(search.NewGroupByDateHistogram("group6", "Col_Date"). // Suppose date format is : 'yyyy-MM-dd HH:mm:ss'
											SetInterval(model.DateTimeValue{Unit: model.DateTimeUnit_HOUR.Enum(), Value: proto.Int32(30)}).
											SetMinDocCount(1).
											SetFiledRange("2023-01-01 12:13:14", "2023-12-31 12:13:14").
											SetMissing("2022-01-06 12:13:14")))
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

	groupByResults = searchResponse.GroupByResults
	group6, err := groupByResults.GroupByDateHistogram("group6")
	if err != nil {
		panic(err)
	}
	fmt.Println("group6: ")
	for _, item := range group6.Items {
		fmt.Printf("\tTimeStamp: %v\tRowCount: %v\n", item.Timestamp, item.RowCount)
	}

	// group by composite
	searchRequest = &tablestore.SearchRequest{}
	searchRequest.
		SetTableName(tableName).
		SetIndexName(indexName).
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). //匹配所有行
			SetLimit(100).
			GroupBy(search.NewGroupByComposite("group7").
				SourceGroupBy(search.NewGroupByField("groupByField", "Col_Keyword")).
				SourceGroupBy(search.NewGroupByHistogram("groupByHistogram", "Col_Long").SetInterval(2)).
				SourceGroupBy(search.NewGroupByDateHistogram("groupByDateHistogram", "Col_Date").SetInterval(model.DateTimeValue{Value: proto.Int32(1), Unit: model.DateTimeUnit_DAY.Enum()})).
				SetSize(5).
				SubAggregation(search.NewSumAggregation("sumAgg", "Col_Double"))))
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

	groupByResults = searchResponse.GroupByResults
	group7, err := groupByResults.GroupByComposite("group7")
	if err != nil {
		panic(err)
	}
	fmt.Println("group7: ")
	if group7.NextToken != nil {
		fmt.Println("\tNextToken: ", *group7.NextToken)
	}
	fmt.Println("\tSourceGroupNames:\n\t" + strings.Join(group7.SourceGroupByNames, "\t"))
	for _, item := range group7.Items {
		keysAsStrings := make([]string, len(item.Keys))
		for i, keyPtr := range item.Keys {
			keysAsStrings[i] = *keyPtr
		}
		fmt.Printf("\t%v, RowCount: %v\n", strings.Join(keysAsStrings, "\t"), item.RowCount)
	}
	//group by geo grid
	group8, err := groupByResults.GroupByGeoGrid("group8") //获取名字为"group7"的GroupBy结果，类型为GroupByGeoGrid
	if err != nil {
		panic(err)
	}
	fmt.Println("group8: ")
	for _, item := range group8.Items {
		fmt.Println("key: ", item.Key, ", geoGrid: ", item.GeoGrid, ", rowCount: ", item.RowCount) //打印返回的value
	}
}

func GroupByFieldSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                               //匹配所有行
			SetLimit(100).                                                   //限制返回前100行结果
			GroupBy(search.NewGroupByField("group_by_field", "Col_Keyword"). //对Col_Keyword字段做GroupByField取值聚合
												GroupBySorters([]search.GroupBySorter{}).                          //可以指定返回结果分桶的顺序
												Size(2).                                                           //仅返回前2个分桶
												SubAggregation(search.NewAvgAggregation("sub_agg1", "Col_Long")).  //对每个分桶进行子统计(Aggregation)
												SubGroupBy(search.NewGroupByField("sub_group1", "Col_Keyword2")))) //对每个分桶进行子聚合(GroupBy)

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	groupByResults := searchResponse.GroupByResults //获取所有聚合结果

	groupByFieldResult, err := groupByResults.GroupByField("group_by_field") //获取名字为"group_by_field"的GroupBy结果，类型为GroupByField
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_field: ")
	for _, item := range groupByFieldResult.Items { //遍历返回的所有分桶
		//item
		fmt.Println("\tkey: ", item.Key, ", rowCount: ", item.RowCount) //打印本次分桶的行数

		//sub agg
		subAgg1, err := item.SubAggregations.Avg("sub_agg1") //获取名字为sub_agg1的子统计的结果
		if err != nil {
			panic(err)
		}
		if subAgg1.HasValue() { //如果子统计sub_agg1计算出了Col_Long字段的平均值，则HasValue()返回true
			fmt.Println("\t\tsub_agg1: ", subAgg1.Value) //打印本次分桶中，子统计计算出来的Col_Long字段的平均值
		}

		//sub group by
		subGroup1, err := item.SubGroupBys.GroupByField("sub_group1") //获取名字为sub_group1的子聚合的结果
		if err != nil {
			panic(err)
		}
		fmt.Println("\t\tsub_group1")
		for _, subItem := range subGroup1.Items { //遍历名字为sub_group1的子聚合结果
			fmt.Println("\t\t\tkey: ", subItem.Key, ", rowCount: ", subItem.RowCount) //打印sub_group1子聚合的结果分桶，即分桶中的行数
			tablestore.Assert(subItem.SubAggregations.Empty(), "")
			tablestore.Assert(subItem.SubGroupBys.Empty(), "")
		}
	}
}

func GroupByRangeSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                            //匹配所有行
			SetLimit(100).                                                //限制返回前100行结果
			GroupBy(search.NewGroupByRange("group_by_range", "Col_Long"). //对Col_Long字段做GroupByRange范围
											Range(search.NegInf, 3). //第一个分桶包含Col_Long在(-∞, 3)的索引行
											Range(3, 5).             //第二个分桶包含Col_Long在[3, 5)的索引行
											Range(5, search.Inf)))   //第三个分桶包含Col_Long在[5, +∞)的索引行

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	groupByResults := searchResponse.GroupByResults //获取所有聚合结果

	groupByRangeResult, err := groupByResults.GroupByRange("group_by_range") //获取名字为"group_by_range"的GroupBy结果，类型为GroupByRange
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_range: ")
	for _, item := range groupByRangeResult.Items { //遍历返回的所有分桶
		fmt.Println("\t[", item.From, ", ", item.To, "), rowCount: ", item.RowCount) //打印本次分桶的行数
	}
}

func GroupByFilterSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                  //匹配所有行
			SetLimit(100).                                      //限制返回前100行结果
			GroupBy(search.NewGroupByFilter("group_by_filter"). //做GroupByFilter过滤聚合
										Query(&search.TermQuery{ //第一个分桶包含Col_Keyword字段取值为"hangzhou"的索引行
					FieldName: "Col_Keyword",
					Term:      "hangzhou",
				}).
				Query(&search.RangeQuery{ //第二个分桶包含Col_Long字段取值在[3, 5]范围的索引行
					FieldName:    "Col_Long",
					From:         3,
					To:           5,
					IncludeLower: true,
					IncludeUpper: true})))

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	groupByResults := searchResponse.GroupByResults //获取所有聚合结果

	groupByFilterResult, err := groupByResults.GroupByFilter("group_by_filter") //获取名字为"group_by_filter"的GroupBy结果，类型为GroupByFilter
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_filter: ")
	for _, item := range groupByFilterResult.Items { //遍历返回的所有分桶
		fmt.Println("\trowCount: ", item.RowCount) //打印本次分桶的行数
	}
}

func GroupByGeoDistanceSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                                                                              //匹配所有行
			SetLimit(100).                                                                                                                  //限制返回前100行结果
			GroupBy(search.NewGroupByGeoDistance("group_by_geo_distance", "Col_GeoPoint", search.GeoPoint{Lat: 30.137817, Lon: 120.08681}). //对Col_GeoPoint字段做GroupByGeoDistance地理范围聚合
																			Range(search.NegInf, 10000). //第一个分桶包含Col_GeoPoint离中心点距离(-∞, 10km)的索引行
																			Range(10000, 15000).         //第二个分桶包含Col_GeoPoint离中心点距离(10km, 15km)的索引行
																			Range(15000, search.Inf)))   //第三个分桶包含Col_GeoPoint离中心点距离(15km, +∞)的索引行

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	groupByResults := searchResponse.GroupByResults //获取所有聚合结果

	groupByGeoDistanceResult, err := groupByResults.GroupByGeoDistance("group_by_geo_distance") //获取名字为"group_by_geo_distance"的GroupBy结果，类型为GroupByGeoDistance
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_geo_distance: ")
	for _, item := range groupByGeoDistanceResult.Items { //遍历返回的所有分桶
		fmt.Println("\t[", item.From, ", ", item.To, "), rowCount: ", item.RowCount) //打印本次分桶的行数
	}
}

func GroupByHistogramSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). //匹配所有行
			SetLimit(100).                     //限制返回前100行结果
			GroupBy(search.NewGroupByHistogram("group_by_histogram", "Col_Long").
				SetInterval(10).
				SetMinDocCount(1).
				SetFiledRange(0, 100).
				SetMissing(3)))

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	groupByResults := searchResponse.GroupByResults //获取所有聚合结果

	groupByHistogramResult, err := groupByResults.GroupByHistogram("group_by_histogram") //获取名字为"group_by_histogram"的GroupBy结果，类型为GroupByHistogram
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_histogram: ")
	for _, item := range groupByHistogramResult.Items {
		fmt.Println("key: ", item.Key.Value, ", value: ", item.Value) //打印返回的value
	}
}

func GroupByDateHistogramSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                             //匹配所有行
			SetLimit(100).                                                                 //限制返回前100行结果
			GroupBy(search.NewGroupByDateHistogram("group_by_date_histogram", "Col_date"). // Suppose date format is : 'yyyy-MM-dd HH:mm:ss'
													SetInterval(model.DateTimeValue{Unit: model.DateTimeUnit_HOUR.Enum(), Value: proto.Int32(30)}).
													SetMinDocCount(1).
													SetFiledRange("2022-01-01 12:13:14", "2022-01-05 12:13:14").
													SetMissing("2022-01-06 12:13:14")))

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	groupByResults := searchResponse.GroupByResults //获取所有聚合结果

	groupByDateHistogramResult, err := groupByResults.GroupByDateHistogram("group_by_date_histogram") //获取名字为"group_by_date_histogram"的GroupBy结果，类型为GroupByHistogram
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_date_histogram: ")
	for _, item := range groupByDateHistogramResult.Items {
		fmt.Println("timestamp: ", item.Timestamp, ", row_count: ", item.RowCount) //打印返回的value
	}
}

func GroupByGeoGridSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). //设置表名
		SetIndexName(indexName). //设置多元索引名
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). //匹配所有行
			SetLimit(100).                     //限制返回前100行结果
			GroupBy(search.NewGroupByGeoGrid("group_by_geo_grid", "Col_geo").
				SetPrecision(model.GHP_156KM_156KM_3).
				SetSize(10)))

	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("RequestId: ", searchResponse.RequestId)
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	groupByResults := searchResponse.GroupByResults //获取所有聚合结果

	groupByGeoGridResult, err := groupByResults.GroupByGeoGrid("group_by_geo_grid") //获取名字为"group_by_geo_grid"的GroupBy结果，类型为GroupByGeoGrid
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_geo_grid: ")
	for _, item := range groupByGeoGridResult.Items {
		fmt.Println("key: ", item.Key, ", geoGrid: ", item.GeoGrid, ", rowCount: ", item.RowCount) //打印返回的value
	}
}

func computeSplits(client *tablestore.TableStoreClient, tableName string, indexName string) (*tablestore.ComputeSplitsResponse, error) {
	req := &tablestore.ComputeSplitsRequest{}
	req.
		SetTableName(tableName).
		SetSearchIndexSplitsOptions(tablestore.SearchIndexSplitsOptions{IndexName: indexName})
	res, err := client.ComputeSplits(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

/**
 * ParallelScan单并发
 */
func ParallelScanSingleConcurrency(client *tablestore.TableStoreClient, tableName string, indexName string) {
	computeSplitsResp, err := computeSplits(client, tableName, indexName)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

	query := search.NewScanQuery().SetQuery(&search.MatchAllQuery{}).SetLimit(2)

	req := &tablestore.ParallelScanRequest{}
	req.SetTableName(tableName).
		SetIndexName(indexName).
		SetColumnsToGet(&tablestore.ColumnsToGet{ReturnAllFromIndex: false}).
		SetScanQuery(query).
		SetSessionId(computeSplitsResp.SessionId)

	res, err := client.ParallelScan(req)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

	total := len(res.Rows)
	for res.NextToken != nil {
		req.SetScanQuery(query.SetToken(res.NextToken))
		res, err = client.ParallelScan(req)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}

		total += len(res.Rows) //process rows each loop
	}
	fmt.Println("total: ", total)
}

/**
 * ParallelScan多并发
 */
func ParallelScanMultiConcurrency(client *tablestore.TableStoreClient, tableName string, indexName string) {
	computeSplitsResp, err := computeSplits(client, tableName, indexName)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

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

			req := &tablestore.ParallelScanRequest{}
			req.SetTableName(tableName).
				SetIndexName(indexName).
				SetColumnsToGet(&tablestore.ColumnsToGet{ReturnAllFromIndex: false}).
				SetScanQuery(query).
				SetSessionId(computeSplitsResp.SessionId)

			res, err := client.ParallelScan(req)
			if err != nil {
				fmt.Printf("%#v", err)
				return
			}

			total := len(res.Rows)
			for res.NextToken != nil {
				req.SetScanQuery(query.SetToken(res.NextToken))
				res, err = client.ParallelScan(req)
				if err != nil {
					fmt.Printf("%#v", err)
					return
				}

				total += len(res.Rows) //process rows each loop
			}
			fmt.Println("total: ", total)
		}()
	}
	wg.Wait()
}

/**
 * 动态修改schema
 * 修改schema的索引必须以_reindex结尾
 */
func UpdateSearchIndexSchema(client *tablestore.TableStoreClient, tableName string, indexName string, indexReindexName string) {
	{
		// step 1.创建索引
		fmt.Println("Begin to create table:", tableName)
		createtableRequest := new(tablestore.CreateTableRequest)
		tableMeta := new(tablestore.TableMeta)
		tableMeta.TableName = tableName
		tableMeta.AddPrimaryKeyColumn("pk1", tablestore.PrimaryKeyType_STRING)
		tableOption := new(tablestore.TableOption)
		tableOption.TimeToAlive = -1
		tableOption.MaxVersion = 1
		reservedThroughput := new(tablestore.ReservedThroughput)
		reservedThroughput.Readcap = 0
		reservedThroughput.Writecap = 0
		createtableRequest.TableMeta = tableMeta
		createtableRequest.TableOption = tableOption
		createtableRequest.ReservedThroughput = reservedThroughput

		_, err := client.CreateTable(createtableRequest)
		if err != nil {
			fmt.Println("Failed to create table with error:", err)
		} else {
			fmt.Println("Create table finished")
		}

		fmt.Println("Begin to create index:", indexName)
		request := &tablestore.CreateSearchIndexRequest{}
		request.TableName = tableName // 设置表名
		request.IndexName = indexName // 设置索引名

		schemas := []*tablestore.FieldSchema{}
		field1 := &tablestore.FieldSchema{
			FieldName:        proto.String("Col_Keyword"),  // 设置字段名，使用proto.String用于获取字符串指针
			FieldType:        tablestore.FieldType_KEYWORD, // 设置字段类型
			Index:            proto.Bool(true),             // 设置开启索引
			EnableSortAndAgg: proto.Bool(true),             // 设置开启排序与统计功能
		}
		field2 := &tablestore.FieldSchema{
			FieldName:        proto.String("Col_Long"),
			FieldType:        tablestore.FieldType_LONG,
			Index:            proto.Bool(true),
			EnableSortAndAgg: proto.Bool(true),
		}
		schemas = append(schemas, field1, field2)

		request.IndexSchema = &tablestore.IndexSchema{
			FieldSchemas: schemas, // 设置SearchIndex包含的字段
		}
		resp, err := client.CreateSearchIndex(request) // 调用client创建SearchIndex
		if err != nil {
			fmt.Println("error :", err)
			return
		}
		fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
	}
	{
		// step 2.创建修改schema后的索引，将field2删除
		fmt.Println("Begin to create index:", indexReindexName)
		request := &tablestore.CreateSearchIndexRequest{}
		request.TableName = tableName        // 设置表名
		request.IndexName = indexReindexName // 设置索引名
		request.SourceIndexName = &indexName // 设置源索引：被修改schema的索引

		schemas := []*tablestore.FieldSchema{}
		field1 := &tablestore.FieldSchema{
			FieldName:        proto.String("Col_Keyword"),  // 设置字段名，使用proto.String用于获取字符串指针
			FieldType:        tablestore.FieldType_KEYWORD, // 设置字段类型
			Index:            proto.Bool(true),             // 设置开启索引
			EnableSortAndAgg: proto.Bool(true),             // 设置开启排序与统计功能
		}
		schemas = append(schemas, field1)

		request.IndexSchema = &tablestore.IndexSchema{
			FieldSchemas: schemas, // 设置SearchIndex包含的字段
		}
		resp, err := client.CreateSearchIndex(request) // 调用client创建SearchIndex
		if err != nil {
			fmt.Println("error :", err)
			return
		}
		fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
	}
	{
		// step 3.设置AB索引权重，权重在0-100
		// 做此步前需要等待"重建索引"数据同步。先后经历"全量同步"和"增量同步"两个阶段
		fmt.Println("wait schema reload")
		time.Sleep(60 * time.Second)
		{
			// 此处原索引权重为50 新索引权重为50
			req := new(tablestore.UpdateSearchIndexRequest)
			req.TableName = tableName
			req.IndexName = indexName
			var queryFlowWeightArray []*tablestore.QueryFlowWeight
			queryFlowWeightArray = append(queryFlowWeightArray, &tablestore.QueryFlowWeight{
				IndexName: indexName,
				Weight:    50,
			})
			queryFlowWeightArray = append(queryFlowWeightArray, &tablestore.QueryFlowWeight{
				IndexName: indexReindexName,
				Weight:    50,
			})
			req.QueryFlowWeights = queryFlowWeightArray
			respU, err := client.UpdateSearchIndex(req)
			if err != nil {
				fmt.Println("update searchIndex failed with error:", err)
			}
			fmt.Println("UpdateSearchIndex finished, requestId:", respU.ResponseInfo.RequestId)
			// 检查权重设置是否成功
			requestD := &tablestore.DescribeSearchIndexRequest{}
			requestD.TableName = tableName
			requestD.IndexName = indexName
			respD, err := client.DescribeSearchIndex(requestD)
			if err != nil {
				fmt.Println("error: ", err)
				return
			}
			if respD.QueryFlowWeights != nil {
				fmt.Printf("QueryFlowWeight:\n")
				for _, queryFlowWeight := range respD.QueryFlowWeights {
					fmt.Printf("%s\n", queryFlowWeight)
				}
			}
		}
		{
			// 此处原索引权重为0 新索引权重为100
			req := new(tablestore.UpdateSearchIndexRequest)
			req.TableName = tableName
			req.IndexName = indexName
			var queryFlowWeightArray []*tablestore.QueryFlowWeight
			queryFlowWeightArray = append(queryFlowWeightArray, &tablestore.QueryFlowWeight{
				IndexName: indexName,
				Weight:    0,
			})
			queryFlowWeightArray = append(queryFlowWeightArray, &tablestore.QueryFlowWeight{
				IndexName: indexReindexName,
				Weight:    100,
			})
			req.QueryFlowWeights = queryFlowWeightArray
			respU, err := client.UpdateSearchIndex(req)
			if err != nil {
				fmt.Println("update searchIndex failed with error:", err)
			}
			fmt.Println("UpdateSearchIndex finished, requestId:", respU.ResponseInfo.RequestId)
			// 检查权重设置是否成功
			requestD := &tablestore.DescribeSearchIndexRequest{}
			requestD.TableName = tableName
			requestD.IndexName = indexName
			respD, err := client.DescribeSearchIndex(requestD)
			if err != nil {
				fmt.Println("error: ", err)
				return
			}
			if respD.QueryFlowWeights != nil {
				fmt.Printf("QueryFlowWeight:\n")
				for _, queryFlowWeight := range respD.QueryFlowWeights {
					fmt.Printf("%s\n", queryFlowWeight)
				}
			}
		}
	}

	{
		// step 4.切换索引, 此时索引schema变为新索引的schema
		switchReq := new(tablestore.UpdateSearchIndexRequest)
		switchReq.TableName = tableName
		switchReq.IndexName = indexName
		switchReq.SwitchIndexName = &indexReindexName
		resp, err := client.UpdateSearchIndex(switchReq)
		if err != nil {
			fmt.Println("update search index failed with error:", err)
		}
		fmt.Println("UpdateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
		// 检查索引切换完后，schema变为新的schema
		requestD := &tablestore.DescribeSearchIndexRequest{}
		requestD.TableName = tableName
		requestD.IndexName = indexName
		respD, err := client.DescribeSearchIndex(requestD)
		if err != nil {
			fmt.Println("error: ", err)
			return
		}
		fmt.Println("FieldSchemas:")
		for _, schema := range respD.Schema.FieldSchemas {
			fmt.Printf("%s\n", schema)
		}

		// 如果发现问题，还有机会切回
		//switchReq := new(tablestore.UpdateSearchIndexRequest)
		//switchReq.TableName = tableName
		//switchReq.IndexName = indexName
		//switchReq.SwitchIndexName = indexReindexName
		//resp, err := client.UpdateSearchIndex(switchReq)
	}
	{
		// step 5.经过一段静默时间后，可以删除修改前的索引
		DeleteSearchIndex(client, tableName, indexReindexName)
	}
}
