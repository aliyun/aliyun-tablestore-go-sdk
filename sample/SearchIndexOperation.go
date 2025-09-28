package sample

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search/model"
	"github.com/golang/protobuf/proto"
)

/**
 * Create a SearchIndex containing two columns: Col_Keyword and Col_Long, with types set to string (KEYWORD) and integer (LONG), respectively.
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
	request.TableName = tableName // Set the table name
	request.IndexName = indexName // Set the index name

	schemas := []*tablestore.FieldSchema{}
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // Set the field name, use proto.String to get a string pointer
		FieldType:        tablestore.FieldType_KEYWORD, // Set the field type
		Index:            proto.Bool(true),             // Set to enable index
		EnableSortAndAgg: proto.Bool(true),             // Enable the sorting and statistics feature
	}
	field2 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Long"),
		FieldType:        tablestore.FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	schemas = append(schemas, field1, field2)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // Set the fields included in the SearchIndex
	}
	resp, err := client.CreateSearchIndex(request) // Call the client to create a SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
}

/**
 * Create a table with a virtual column SearchIndex
 * It includes two base columns, Col_Keyword and Col_Long, which are set to the types string (KEYWORD) and integer (LONG), respectively.
 * Col_long_str is a virtual column of type string (KEYWORD) that maps to the original column Col_long.
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
	request.TableName = tableName // Set the table name
	request.IndexName = indexName // Set the index name

	schemas := []*tablestore.FieldSchema{}
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // Set the field name, use proto.String to get a string pointer
		FieldType:        tablestore.FieldType_KEYWORD, // Set the field type
		Index:            proto.Bool(true),             // Set to enable indexing
		EnableSortAndAgg: proto.Bool(true),             // Set to enable the sorting and statistics feature
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
		IsVirtualField:   proto.Bool(true),     // Set the field type to virtual column
		SourceFieldNames: []string{"Col_Long"}, // Set the original column for virtual column mapping
	}
	schemas = append(schemas, field1, field2, field3)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // Set the fields included in the SearchIndex
	}
	resp, err := client.CreateSearchIndex(request) // Call the client to create a SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
}

/**
 * Create a SearchIndex that includes two columns: Col_Keyword and Col_Long. Their types are set to string (KEYWORD) and integer (LONG), respectively. Set the pre-sorting based on the Col_Long column.
 */
func CreateSearchIndexWithIndexSort(client *tablestore.TableStoreClient, tableName string, indexName string) {
	fmt.Println("Begin to create index:", indexName)
	request := &tablestore.CreateSearchIndexRequest{}
	request.TableName = tableName // Set the table name
	request.IndexName = indexName // Set the index name

	schemas := []*tablestore.FieldSchema{}
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // Set the field name, use proto.String to get a string pointer
		FieldType:        tablestore.FieldType_KEYWORD, // Set the field type
		Index:            proto.Bool(true),             // Set to enable index
		EnableSortAndAgg: proto.Bool(true),             // Set to enable sorting and statistics functionality
	}
	field2 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Long"),
		FieldType:        tablestore.FieldType_LONG,
		Index:            proto.Bool(true),
		EnableSortAndAgg: proto.Bool(true),
	}
	schemas = append(schemas, field1, field2)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // Set the fields included in the SearchIndex
		IndexSort: &search.Sort{ // Set the index sort, reverse sort by the value of Col_Long.
			Sorters: []search.Sorter{
				&search.FieldSort{
					FieldName: "Col_Long",
					Order:     search.SortOrder_ASC.Enum(),
				},
			},
		},
	}
	resp, err := client.CreateSearchIndex(request) // Call the client to create a SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
}

// Create a SearchIndex to prepare for the query highlighting demo.
func CreateSearchIndexForQueryHighlighting(client *tablestore.TableStoreClient, tableName string, indexName string) {
	var schemas []*tablestore.FieldSchema
	field1 := &tablestore.FieldSchema{
		FieldName:          proto.String("Col_Text"),  // Set the field name, use proto.String to get a string pointer.
		FieldType:          tablestore.FieldType_TEXT, // Set the field type
		Index:              proto.Bool(true),          // Set to enable indexing
		EnableHighlighting: proto.Bool(true),          // Set to enable field highlighting
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

// WriteDataForVectorQuery inserts data for highlight query testing.
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
		FieldName:     "col_vector",
		NumCandidates: proto.Int32(15),
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
		TopK:               proto.Int32(10),
		MinScore:           proto.Float32(0.5),
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
		FieldName:        proto.String("Col_Keyword"),  // Set the field name, use proto.String to get a string pointer.
		FieldType:        tablestore.FieldType_KEYWORD, // Set the field type
		Index:            proto.Bool(true),             // Set to enable indexing
		EnableSortAndAgg: proto.Bool(true),             // Set to enable the sorting and statistics feature
	}

	schemas = append(schemas, field1)

	createSearchIndex(client, tableName, indexName, schemas)
}

/**
 * Create a SearchIndex to prepare for the Aggregation and GroupBy demo.
 */
func CreateSearchIndexForAggregationAndGroupBy(client *tablestore.TableStoreClient, tableName string, indexName string) {
	var schemas []*tablestore.FieldSchema
	field1 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword"),  // Set the field name, use proto.String to get a string pointer
		FieldType:        tablestore.FieldType_KEYWORD, // Set the field type
		Index:            proto.Bool(true),             // Set to enable indexing
		EnableSortAndAgg: proto.Bool(true),             // Enable the sorting and statistics feature
	}
	field2 := &tablestore.FieldSchema{
		FieldName:        proto.String("Col_Keyword2"), // Set the field name, use proto.String to get a string pointer
		FieldType:        tablestore.FieldType_KEYWORD, // Set the field type
		Index:            proto.Bool(true),             // Set to enable index
		EnableSortAndAgg: proto.Bool(true),             // Set to enable sorting and statistics functionality
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
	request.TableName = tableName // Set the table name
	request.IndexName = indexName // Set the index name
	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: fieldSchemas,
	}

	resp, err := client.CreateSearchIndex(request) // Call the client to create a SearchIndex
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

	// If includeSyncStat is set to false, the returned result will not contain SyncStat information. Not setting it or setting it to true will both return SyncStat normally.
	//FalseBoolean := false
	//request.IncludeSyncStat = &FalseBoolean

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

// WriteDataForQueryHighlighting inserts data for highlighting query testing.
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

// QueryHighlightingSample Query highlighting example
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
 * Insert data for Aggregation and GroupBy testing
 */
func WriteDataForAggregationAndGroupBy(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("Begin to write data")
	keywords := []string{"hangzhou", "tablestore", "ots"}
	keywords2 := []string{"red", "blue"}
	geopoints := []string{
		"30.137817,120.08681",  // Flying Heaven Park
		"30.135131,120.088355", // Grand Silver Center
		"30.181877,120.152818", // Traditional Chinese Medicine subway station
		"30.20223,120.13787",   // Liuhe Pagoda
		"30.216961,120.157633", // Eight-Trigram Field
		"30.231566,120.148578", //太子湾
		"30.26058,120.170712",  // Longxiang Bridge
		"30.269501,120.169347", // Fengqilu
		"30.28073,120.168843",  // Canal
		"30.296946,120.21958",  // Hangzhou East Railway Station
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
 * Use Token for paginated reading.
 * If NextToken is returned in SearchResponse, you can use this Token to initiate the next query,
 * until NextToken is empty (nil), which indicates that all data meeting the conditions has been read.
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
	searchQuery.SetGetTotalCount(true) // Setting GetTotalCount to true will return the total count.
	searchRequest.SetSearchQuery(searchQuery)
	searchRequest.SetTimeoutMs(30000) // You can explicitly set the request timeout.
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
				MissingField: proto.String("Col_Long_Sec"), // If the sorting field Col_Long is missing, replace it with Col_Long_Sec.
				MissingValue: 50,                           // Replace with missingValue if both the sort field and the replacement field are missing.
				// MissingValue: search.FirstWhenMissing, // If missingValue is set to FirstWhenMissing, the entries with missing sort field values will be placed at the front.
			},
		},
	})
	searchQuery.SetLimit(10)
	searchRequest.SetSearchQuery(searchQuery)
	searchRequest.SetTimeoutMs(30000) // You can explicitly set the request timeout.
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
 * Query the values in the Col_Keyword column of the table that match "hangzhou", and return the total number of matching rows and some successfully matched rows.
 */
func MatchQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.MatchQuery{}   // Set the query type to MatchQuery
	query.FieldName = "Col_Keyword" // Set the field to match
	query.Text = "hangzhou"         // Set the value to match
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetOffset(0) // Set the offset to 0
	searchQuery.SetLimit(20) // Set the limit to 20, which means a maximum of 20 data entries will be returned.
	searchRequest.SetSearchQuery(searchQuery)
	searchResponse, err := client.Search(searchRequest)
	if err != nil { // Judge the exception
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the return result is complete
	fmt.Println("TotalCount: ", searchResponse.TotalCount)     // Total number of matched rows
	fmt.Println("RowCount: ", len(searchResponse.Rows))        // The number of rows returned
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody)) // If columnsToGet is not set, only the primary key will be returned by default.
	}
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Query the values in the Col_Text column of the table that match "hangzhou shanghai", with the matching condition being phrase matching (requires the phrase to be matched in full and in order), and return the total number of rows matched and some successfully matched rows.
 */
func MatchPhraseQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.MatchPhraseQuery{} // Set the query type to MatchPhraseQuery
	query.FieldName = "Col_Text"        // Set the field to match
	query.Text = "hangzhou shanghai"    // Set the value to match
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetOffset(0) // Set the offset to 0
	searchQuery.SetLimit(20) // Set the limit to 20, which means a maximum of 20 data entries will be returned.
	searchRequest.SetSearchQuery(searchQuery)
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Query the data in the Col_Keyword column of the table that exactly matches "hangzhou".
 */
func TermQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.TermQuery{}    // Set the query type to TermQuery
	query.FieldName = "Col_Keyword" // Set the field to match
	query.Term = "hangzhou"         // Set the value to match
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetLimit(100)
	searchRequest.SetSearchQuery(searchQuery)
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Query the data in the Col_Keyword column of the table that exactly matches "hangzhou" or "tablestore".
 */
func TermsQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.TermsQuery{}   // Set the query type to TermQuery
	query.FieldName = "Col_Keyword" // Set the field to match
	terms := make([]interface{}, 0)
	terms = append(terms, "hangzhou")
	terms = append(terms, "tablestore")
	query.Terms = terms // Set the value to match
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetLimit(100)
	searchRequest.SetSearchQuery(searchQuery)
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Query the data in the Col_Keyword column of the table where the prefix is "hangzhou".
 */
func PrefixQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.PrefixQuery{}  // Set the query type to PrefixQuery
	query.FieldName = "Col_Keyword" // Set the field to match
	query.Prefix = "hangzhou"       // Set prefix
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Use wildcard query to search for data where the value of the Col_Keyword column in the table matches "hang*u".
 */
func WildcardQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.WildcardQuery{} // Set the query type to WildcardQuery
	query.FieldName = "Col_Keyword"
	query.Value = "hang*u"
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Query the data where the Col_Long column is greater than 3 in the table, and sort the results in descending order by the value of the Col_Long column.
 */
func RangeQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	searchQuery := search.NewSearchQuery()
	rangeQuery := &search.RangeQuery{} // Set the query type to RangeQuery
	rangeQuery.FieldName = "Col_Long"  // Set the target field
	rangeQuery.GT(3)                   // Set the range condition for this field to be greater than 3.
	searchQuery.SetQuery(rangeQuery)
	// Set reverse order sorting by the Col_Long column
	searchQuery.SetSort(&search.Sort{
		Sorters: []search.Sorter{
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
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Col_GeoPoint is of GeoPoint type. This query retrieves data within the rectangular range defined by the top-left corner at "10,0" and the bottom-right corner at "0,10" for the Col_GeoPoint column in the table.
 */
func GeoBoundingBoxQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.GeoBoundingBoxQuery{} // Set the query type to GeoBoundingBoxQuery
	query.FieldName = "Col_GeoPoint"       // Set which field's value to compare
	query.TopLeft = "10,0"                 // Set the top-left corner of the rectangle
	query.BottomRight = "0,10"             // Set the bottom-right corner of the rectangle
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Query the data where the values in the Col_GeoPoint column of the table are within a certain distance from the center point.
 */
func GeoDistanceQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.GeoDistanceQuery{} // Set the query type to GeoDistanceQuery
	query.FieldName = "Col_GeoPoint"
	query.CenterPoint = "5,5"       // Set the center point
	query.DistanceInMeter = 10000.0 // Set the distance condition to the center point, not exceeding 10,000 meters
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Query the data where the values of the Col_GeoPoint column in the table are within a given polygon range.
 */
func GeoPolygonQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.GeoPolygonQuery{} // Set the query type to GeoDistanceQuery
	query.FieldName = "Col_GeoPoint"
	query.Points = []string{"0,0", "5,5", "5,0"} // Set the vertices of the polygon
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Perform composite condition queries through BoolQuery.
 */
func BoolQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)

	/**
	 * Query condition one: RangeQuery, the value of the Col_Long column must be greater than 3.
	 */
	rangeQuery := &search.RangeQuery{}
	rangeQuery.FieldName = "Col_Long"
	rangeQuery.GT(3)

	/**
	 * Query condition two: MatchQuery, the value of the Col_Keyword column should match "hangzhou"
	 */
	matchQuery := &search.MatchQuery{}
	matchQuery.FieldName = "Col_Keyword"
	matchQuery.Text = "hangzhou"

	{
		/**
		 * Constructs a BoolQuery, setting the query condition to require both "Condition One" and "Condition Two" to be met simultaneously.
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
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the return result is complete
		fmt.Println("RowCount: ", len(searchResponse.Rows))
	}
	{
		/**
		 * Construct a BoolQuery, setting the query condition to satisfy at least one of "condition one" or "condition two".
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
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the return result is complete
		fmt.Println("RowCount: ", len(searchResponse.Rows))
	}
}

/**
 * Create a SearchIndex and customize a tokenizer for the TEXT type index column.
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
	request.TableName = tableName // Set the table name
	request.IndexName = indexName // Set the index name

	schemas := []*tablestore.FieldSchema{}

	analyzer1 := tablestore.Analyzer_SingleWord
	analyzerParam1 := tablestore.SingleWordAnalyzerParameter{
		CaseSensitive: proto.Bool(true),
		DelimitWord:   proto.Bool(true),
	}
	field1 := &tablestore.FieldSchema{
		FieldName:         proto.String("Col_SingleWord"), // Set the field name, use proto.String to get a string pointer
		FieldType:         tablestore.FieldType_TEXT,      // Set the field type
		Index:             proto.Bool(true),               // Set to enable indexing
		Analyzer:          &analyzer1,                     // Set the tokenizer
		AnalyzerParameter: analyzerParam1,                 // Set the tokenizer parameters (optional)
	}

	analyzer2 := tablestore.Analyzer_MaxWord
	field2 := &tablestore.FieldSchema{
		FieldName: proto.String("Col_MaxWord"), // Set the field name, use proto.String to get a string pointer
		FieldType: tablestore.FieldType_TEXT,   // Set the field type
		Index:     proto.Bool(true),            // Set to enable index
		Analyzer:  &analyzer2,                  // Set the tokenizer
	}

	analyzer3 := tablestore.Analyzer_MinWord
	field3 := &tablestore.FieldSchema{
		FieldName: proto.String("Col_MinWord"), // Set the field name, use proto.String to get a string pointer.
		FieldType: tablestore.FieldType_TEXT,   // Set the field type
		Index:     proto.Bool(true),            // Set to enable index
		Analyzer:  &analyzer3,                  // Set the tokenizer
	}

	analyzer4 := tablestore.Analyzer_Split
	analyzerParam4 := tablestore.SplitAnalyzerParameter{Delimiter: proto.String("-")}
	field4 := &tablestore.FieldSchema{
		FieldName:         proto.String("Col_Split"), // Set the field name, use proto.String to get a string pointer.
		FieldType:         tablestore.FieldType_TEXT, // Set the field type
		Index:             proto.Bool(true),          // Set to enable indexing
		Analyzer:          &analyzer4,                // Set the tokenizer
		AnalyzerParameter: analyzerParam4,            // Set the tokenizer parameters (optional)
	}

	analyzer5 := tablestore.Analyzer_Fuzzy
	analyzerParam5 := tablestore.FuzzyAnalyzerParameter{
		MinChars: 1,
		MaxChars: 4,
	}
	field5 := &tablestore.FieldSchema{
		FieldName:         proto.String("Col_Fuzzy"), // Set the field name, use proto.String to get a string pointer
		FieldType:         tablestore.FieldType_TEXT, // Set the field type
		Index:             proto.Bool(true),          // Set to enable index
		Analyzer:          &analyzer5,                // Set the tokenizer
		AnalyzerParameter: analyzerParam5,            // Set the tokenizer parameters (optional)
	}

	schemas = append(schemas, field1, field2, field3, field4, field5)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // Set the fields included in the SearchIndex
	}
	resp, err := client.CreateSearchIndex(request) // Call the client to create a SearchIndex
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
		query := &search.MatchQuery{}      // Set the query type to MatchQuery
		query.FieldName = "Col_SingleWord" // Set the field to match
		query.Text = "歌"                   // Set the value to match
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchRequest.SetSearchQuery(searchQuery)

		// Set to return all columns
		searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
			ReturnAll: true,
		})
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
		query := &search.MatchQuery{}   // Set the query type to MatchQuery
		query.FieldName = "Col_MaxWord" // Set the field to match
		query.Text = "中华人民共和国"          // Set the value to match
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchRequest.SetSearchQuery(searchQuery)

		// Set to return all columns
		searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
			ReturnAll: true,
		})
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
		query := &search.MatchQuery{} // Set the query type to MatchQuery
		query.FieldName = "Col_Split" // Set the field to match
		query.Text = "2019"           // Set the value to match
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchRequest.SetSearchQuery(searchQuery)

		// Set to return all columns
		searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
			ReturnAll: true,
		})
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
		query := &search.MatchQuery{} // Set the query type to MatchQuery
		query.FieldName = "Col_Fuzzy" // Set the field to match
		query.Text = "程"              // Set the value to match
		searchQuery := search.NewSearchQuery()
		searchQuery.SetQuery(query)
		searchRequest.SetSearchQuery(searchQuery)

		// Set to return all columns
		searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
			ReturnAll: true,
		})
		searchResponse, err := client.Search(searchRequest)
		if err != nil {
			fmt.Printf("%#v", err)
			return
		}
		fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
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
 * Aggregation example
 */
func AggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	var percentiles = make([]float64, 3)
	percentiles[0] = 0.0
	percentiles[1] = 50.0
	percentiles[2] = 100.0

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                   // Match all rows
			SetLimit(100).                                                       // Limit to returning the first 100 rows of results
			Aggregation(search.NewAvgAggregation("agg1", "Col_Long")).           // Calculate the average value of the Col_Long field
			Aggregation(search.NewDistinctCountAggregation("agg2", "Col_Long")). // Calculate the number of different values for the Col_Long field.
			Aggregation(search.NewMaxAggregation("agg3", "Col_Long")).           // Calculate the maximum value of the Col_Long field.
			Aggregation(search.NewSumAggregation("agg4", "Col_Long")).           // Calculate the sum of the Col_Long field
			Aggregation(search.NewCountAggregation("agg5", "Col_Long")).         // Calculate the number of rows where the Col_Long field exists.
			Aggregation(search.NewTopRowsAggregation("agg6").SetLimit(1).SetSort(&search.Sort{
				Sorters: []search.Sorter{
					&search.FieldSort{
						FieldName: "Col_Long",
						Order:     search.SortOrder_DESC.Enum(),
					},
				},
			})).
			Aggregation(search.NewPercentilesAggregation("agg7", "Col_Long").SetMissing(10).SetPercents(percentiles)))

	// Set to return all columns
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
	aggResults := searchResponse.AggregationResults // Get all statistical results

	//avg agg
	agg1, err := aggResults.Avg("agg1") // Get the Aggregation result named "agg1", the type is Avg
	if err != nil {
		panic(err)
	}
	if agg1.HasValue() { // Whether the Value of the Aggregation result named "agg1" is valid
		fmt.Println("(avg) agg1: ", agg1.Value) // Print the average value of the Col_Long field
	} else {
		fmt.Println("(avg) agg1: no value") // The Col_Long field does not exist in any row.
	}

	//distinct count agg
	agg2, err := aggResults.DistinctCount("agg2") // Get the Aggregation result named "agg2", the type is DistinctCount.
	if err != nil {
		panic(err)
	}
	fmt.Println("(distinct) agg2: ", agg2.Value) // Print the number of different values of the Col_Long field

	//max agg
	agg3, err := aggResults.Max("agg3") // Get the Aggregation result named "agg3", the type is Max.
	if err != nil {
		panic(err)
	}
	if agg3.HasValue() {
		fmt.Println("(max) agg3: ", agg3.Value) // Print the maximum value of the Col_Long field
	} else {
		fmt.Println("(max) agg3: no value") // The Col_Long field does not exist in any row.
	}

	//sum agg
	agg4, err := aggResults.Sum("agg4") // Get the Aggregation result named "agg4", the type is Sum.
	if err != nil {
		panic(err)
	}
	fmt.Println("(sum) agg4: ", agg4.Value) // Print the sum of the Col_Long field

	//count agg
	agg5, err := aggResults.Count("agg5") // Get the Aggregation result named "agg5", the type is Count.
	if err != nil {
		panic(err)
	}
	fmt.Println("(count) agg5: ", agg5.Value) // Print the number of existing Col_Long fields

	//topRows agg
	agg6, err := aggResults.TopRows("agg6") // Get the Aggregation result named "agg6", the type is TopRows.
	if err != nil {
		panic(err)
	}
	jsonBody, err := json.Marshal(agg6.Value)
	if err != nil {
		panic(err)
	}
	fmt.Println("TowRow: ", string(jsonBody)) // Print the returned row

	//percentiles agg
	agg7, err := aggResults.Percentiles("agg7") // Get the Aggregation result named "agg7", with the type of Percentiles.
	if err != nil {
		panic(err)
	}
	for _, item := range agg7.PercentilesAggregationItems {
		fmt.Println("\t(percentiles)key: ", item.Key, ", value: ", item.Value.Value) // Print the returned value
	}
}

func AvgAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                            // Match all rows
			SetLimit(100).                                                // Limit to returning the first 100 rows of results
			Aggregation(search.NewAvgAggregation("avg_agg", "Col_Long"))) // Calculate the average value of the Col_Long field
	// Set to return all columns
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
	aggResults := searchResponse.AggregationResults // Get all statistical results

	//avg agg
	aggregationResult, err := aggResults.Avg("avg_agg") // Get the Aggregation result named "avg_agg", the type is Avg
	if err != nil {
		panic(err)
	}
	if aggregationResult.HasValue() {
		fmt.Println("avg_agg: ", aggregationResult.Value) // Print the average value of the Col_Long field
	} else {
		fmt.Println("avg_agg: no value") // The Col_Long field does not exist in any row.
	}
}

func DistinctAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                                 // Match all rows
			SetLimit(100).                                                                     // Limit to returning the first 100 rows of results
			Aggregation(search.NewDistinctCountAggregation("distinct_count_agg", "Col_Long"))) // Calculate the number of different values for the Col_Long field.
	// Set to return all columns
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
	aggResults := searchResponse.AggregationResults // Get all statistical results

	aggregationResult, err := aggResults.DistinctCount("distinct_count_agg") // Get the Aggregation result named "distinct_count_agg", the type is DistinctCount.
	if err != nil {
		panic(err)
	}
	fmt.Println("distinct_count_agg: ", aggregationResult.Value) // Print the number of different values of the Col_Long field
}

func MaxAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                            // Match all rows
			SetLimit(100).                                                // Limit to returning the first 100 rows of results
			Aggregation(search.NewMaxAggregation("max_agg", "Col_Long"))) // Calculate the maximum value of the Col_Long field
	// Set to return all columns
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
	aggResults := searchResponse.AggregationResults // Get all statistical results

	aggregationResult, err := aggResults.Max("max_agg") // Get the Aggregation result named "max_agg", the type is Max.
	if err != nil {
		panic(err)
	}
	if aggregationResult.HasValue() {
		fmt.Println("max_agg: ", aggregationResult.Value) // Print the maximum value of the Col_Long field
	} else {
		fmt.Println("max_agg: no value") // The Col_Long field does not exist in any row.
	}
}

func SumAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                            // Match all rows
			SetLimit(100).                                                // Limit to returning the first 100 rows of results
			Aggregation(search.NewSumAggregation("sum_agg", "Col_Long"))) // Calculate the sum of the Col_Long field
	// Set to return all columns
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
	aggResults := searchResponse.AggregationResults // Get all statistical results

	aggregationResult, err := aggResults.Sum("sum_agg") // Get the Aggregation result named "sum_agg", the type is Sum.
	if err != nil {
		panic(err)
	}
	fmt.Println("sum_agg: ", aggregationResult.Value) // Print the sum of the Col_Long field
}

func CountAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                // Match all rows
			SetLimit(100).                                                    // Limit to returning the first 100 rows of results
			Aggregation(search.NewCountAggregation("count_agg", "Col_Long"))) // Calculate the number of rows where the Col_Long field exists.
	// Set to return all columns
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
	aggResults := searchResponse.AggregationResults // Get all statistical results

	aggregationResult, err := aggResults.Count("count_agg") // Get the Aggregation result named "count_agg", the type is Count.
	if err != nil {
		panic(err)
	}
	fmt.Println("count_agg: ", aggregationResult.Value) // Print the number of existing Col_Long fields
}

func TopRowsAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). // Match all rows
			SetLimit(100).                     // Limit to returning the first 100 rows of results
			Aggregation(search.NewTopRowsAggregation("top_rows_agg").SetLimit(1).SetSort(&search.Sort{
				Sorters: []search.Sorter{
					&search.FieldSort{
						FieldName: "Col_Long",
						Order:     search.SortOrder_DESC.Enum(),
					},
				},
			})))
	// Set to return all columns
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
	aggResults := searchResponse.AggregationResults // Get all statistical results

	aggregationResult, err := aggResults.TopRows("top_rows_agg") // Get the Aggregation result named "top_rows_agg", the type is TopRows.
	if err != nil {
		panic(err)
	}
	jsonBody, err := json.Marshal(aggregationResult.Value)
	if err != nil {
		panic(err)
	}
	fmt.Println("top_rows_agg: ", string(jsonBody)) // Print the returned row
}

func PercentilesAggregationSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	var percentiles = make([]float64, 3)
	percentiles[0] = 0.0
	percentiles[1] = 50.0
	percentiles[2] = 100.0

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). // Match all rows
			SetLimit(100).                     // Limit to returning the first 100 rows of results
			Aggregation(search.NewPercentilesAggregation("percentiles_agg", "Col_Long").SetMissing(10).SetPercents(percentiles)))

	// Set to return all columns
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
	aggResults := searchResponse.AggregationResults // Get all statistical results

	aggregationResult, err := aggResults.Percentiles("percentiles_agg") // Get the Aggregation result named "percentiles_agg", the type is Percentiles.
	if err != nil {
		panic(err)
	}
	for _, item := range aggregationResult.PercentilesAggregationItems {
		fmt.Println("\tkey: ", item.Key, ", value: ", item.Value.Value) // Print the returned value
	}
}

/**
 * GroupBy example
 */
func GroupBySample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                       // Match all rows
			SetLimit(100).                                           // Limit to returning the first 100 rows of results
			GroupBy(search.NewGroupByField("group1", "Col_Keyword"). // Perform value aggregation on the Col_Keyword field using GroupByField.
											GroupBySorters([]search.GroupBySorter{}).                          // You can specify the order of the buckets for the returned results.
											Size(2).                                                           // Return only the first 2 split buckets.
											SubAggregation(search.NewAvgAggregation("sub_agg1", "Col_Long")).  // Perform sub-statistics (Aggregation) on each bucket.
											SubGroupBy(search.NewGroupByField("sub_group1", "Col_Keyword2"))). // Perform sub-aggregation (GroupBy) on each bucket.
			GroupBy(search.NewGroupByRange("group2", "Col_Long").    // Perform GroupByRange on the Col_Long field
											Range(search.NegInf, 3). // The first bucket contains index rows where Col_Long is in (-∞, 3).
											Range(3, 5).             // The second bucket contains index rows with Col_Long in [3, 5).
											Range(5, search.Inf)).   // The third bucket contains index rows with Col_Long in [5, +∞).
			GroupBy(search.NewGroupByFilter("group3").               // Perform GroupByFilter aggregation filtering
											Query(&search.TermQuery{ // The first bucket contains the index rows where the Col_Keyword field has a value of "hangzhou".
					FieldName: "Col_Keyword",
					Term:      "hangzhou",
				}).
				Query(&search.RangeQuery{ // The second bucket contains index rows with Col_Long field values in the range [3, 5].
																			FieldName:    "Col_Long",
																			From:         3,
																			To:           5,
																			IncludeLower: true,
																			IncludeUpper: true})).
			GroupBy(search.NewGroupByGeoDistance("group4", "Col_GeoPoint", search.GeoPoint{Lat: 30.137817, Lon: 120.08681}). // Perform GroupByGeoDistance geographic range aggregation on the Col_GeoPoint field
																		Range(search.NegInf, 10000). // The first bucket contains the index rows for Col_GeoPoint with distances from the center point in the range (-∞, 10km).
																		Range(10000, 15000).         // The second bucket contains the index rows for Col_GeoPoint with distances from the center point (10km, 15km).
																		Range(15000, search.Inf)).   // The third bucket contains the index rows where the distance between Col_GeoPoint and the center point is within (15km, +∞).
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

	// Set to return all columns
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
	groupByResults := searchResponse.GroupByResults // Get all aggregation results

	group1, err := groupByResults.GroupByField("group1") // Get the GroupBy result named "group1", the type is GroupByField
	if err != nil {
		panic(err)
	}
	fmt.Println("group1: ")
	for _, item := range group1.Items { // Iterate through all the returned buckets.
		//item
		fmt.Println("\tkey: ", item.Key, ", rowCount: ", item.RowCount) // Print the number of rows for this partition bucket

		//sub agg
		subAgg1, err := item.SubAggregations.Avg("sub_agg1") // Get the result of the sub-statistic named sub_agg1
		if err != nil {
			panic(err)
		}
		if subAgg1.HasValue() { // If the sub-statistic sub_agg1 calculates the average value of the Col_Long field, HasValue() returns true.
			fmt.Println("\t\tsub_agg1: ", subAgg1.Value) // Print the average value of the Col_Long field calculated by the sub-statistics in this partition bucket.
		}

		//sub group by
		subGroup1, err := item.SubGroupBys.GroupByField("sub_group1") // Get the result of the sub-aggregation named sub_group1
		if err != nil {
			panic(err)
		}
		fmt.Println("\t\tsub_group1")
		for _, subItem := range subGroup1.Items { // Iterate through the sub-aggregation results named sub_group1
			fmt.Println("\t\t\tkey: ", subItem.Key, ", rowCount: ", subItem.RowCount) // Print the result buckets of the sub_group1 sub-aggregation, i.e., the number of rows in each bucket.
			tablestore.Assert(subItem.SubAggregations.Empty(), "")
			tablestore.Assert(subItem.SubGroupBys.Empty(), "")
		}
	}

	//group by range
	group2, err := groupByResults.GroupByRange("group2") // Get the GroupBy result named "group2", the type is GroupByRange
	if err != nil {
		panic(err)
	}
	fmt.Println("group2: ")
	for _, item := range group2.Items { // Iterate through all the returned buckets.
		fmt.Println("\t[", item.From, ", ", item.To, "), rowCount: ", item.RowCount) // Print the number of rows for this partition bucket
	}

	//group by filter
	group3, err := groupByResults.GroupByFilter("group3") // Get the GroupBy result named "group3", the type is GroupByFilter.
	if err != nil {
		panic(err)
	}
	fmt.Println("group3: ")
	for _, item := range group3.Items { // Iterate through all the returned buckets.
		fmt.Println("\trowCount: ", item.RowCount) // Print the number of rows for this partition bucket
	}

	//group by geo distance
	group4, err := groupByResults.GroupByGeoDistance("group4") // Get the GroupBy result named "group4", the type is GroupByGeoDistance
	if err != nil {
		panic(err)
	}
	fmt.Println("group4: ")
	for _, item := range group4.Items { // Iterate through all the returned buckets.
		fmt.Println("\t[", item.From, ", ", item.To, "), rowCount: ", item.RowCount) // Print the number of rows for this partition bucket
	}

	//group by histogram
	group5, err := groupByResults.GroupByHistogram("group5") // Get the GroupBy result named "group5", the type is GroupByHistogram
	if err != nil {
		panic(err)
	}
	fmt.Println("group5: ")
	for _, item := range group5.Items {
		fmt.Println("key: ", item.Key.Value, ", value: ", item.Value) // Print the returned value
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
			SetQuery(&search.MatchAllQuery{}). // Match all rows
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
	group8, err := groupByResults.GroupByGeoGrid("group8") // Get the GroupBy result named "group7", the type is GroupByGeoGrid
	if err != nil {
		panic(err)
	}
	fmt.Println("group8: ")
	for _, item := range group8.Items {
		fmt.Println("key: ", item.Key, ", geoGrid: ", item.GeoGrid, ", rowCount: ", item.RowCount) // Print the returned value
	}
}

func GroupByFieldSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                               // Match all rows
			SetLimit(100).                                                   // Limit to returning the first 100 rows of results
			GroupBy(search.NewGroupByField("group_by_field", "Col_Keyword"). // Perform value aggregation on the Col_Keyword field using GroupByField
												GroupBySorters([]search.GroupBySorter{}).                          // You can specify the order of the buckets for the return results.
												Size(2).                                                           // Return only the first 2 partitions.
												SubAggregation(search.NewAvgAggregation("sub_agg1", "Col_Long")).  // Perform sub-statistics (Aggregation) on each bucket.
												SubGroupBy(search.NewGroupByField("sub_group1", "Col_Keyword2")))) // Perform sub-aggregation (GroupBy) on each bucket.

	// Set to return all columns
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
	groupByResults := searchResponse.GroupByResults // Get all aggregation results

	groupByFieldResult, err := groupByResults.GroupByField("group_by_field") // Get the GroupBy result named "group_by_field", the type is GroupByField
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_field: ")
	for _, item := range groupByFieldResult.Items { // Iterate through all the returned buckets.
		//item
		fmt.Println("\tkey: ", item.Key, ", rowCount: ", item.RowCount) // Print the number of rows for this partition bucket

		//sub agg
		subAgg1, err := item.SubAggregations.Avg("sub_agg1") // Get the result of the sub-statistic named sub_agg1
		if err != nil {
			panic(err)
		}
		if subAgg1.HasValue() { // If the sub-statistic sub_agg1 calculates the average value of the Col_Long field, then HasValue() returns true.
			fmt.Println("\t\tsub_agg1: ", subAgg1.Value) // Print the average value of the Col_Long field calculated by the sub-statistics in this partition bucket.
		}

		//sub group by
		subGroup1, err := item.SubGroupBys.GroupByField("sub_group1") // Get the result of the sub-aggregation named sub_group1
		if err != nil {
			panic(err)
		}
		fmt.Println("\t\tsub_group1")
		for _, subItem := range subGroup1.Items { // Iterate through the sub-aggregation results named sub_group1
			fmt.Println("\t\t\tkey: ", subItem.Key, ", rowCount: ", subItem.RowCount) // Print the result buckets of the sub_group1 sub-aggregation, i.e., the number of rows in each bucket.
			tablestore.Assert(subItem.SubAggregations.Empty(), "")
			tablestore.Assert(subItem.SubGroupBys.Empty(), "")
		}
	}
}

func GroupByRangeSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                            // Match all rows
			SetLimit(100).                                                // Limit to returning the first 100 rows of results
			GroupBy(search.NewGroupByRange("group_by_range", "Col_Long"). // Perform GroupByRange on the Col_Long field
											Range(search.NegInf, 3). // The first bucket contains the index rows where Col_Long is in (-∞, 3).
											Range(3, 5).             // The second bucket contains the index rows where Col_Long is in [3, 5).
											Range(5, search.Inf)))   // The third bucket contains the index rows where Col_Long is in [5, +∞).

	// Set to return all columns
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
	groupByResults := searchResponse.GroupByResults // Get all aggregation results

	groupByRangeResult, err := groupByResults.GroupByRange("group_by_range") // Get the GroupBy result named "group_by_range", the type is GroupByRange.
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_range: ")
	for _, item := range groupByRangeResult.Items { // Iterate through all the returned buckets.
		fmt.Println("\t[", item.From, ", ", item.To, "), rowCount: ", item.RowCount) // Print the number of rows for this partition bucket
	}
}

func GroupByFilterSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                  // Match all rows
			SetLimit(100).                                      // Limit to returning the first 100 rows of results
			GroupBy(search.NewGroupByFilter("group_by_filter"). // Perform GroupByFilter aggregation filtering
										Query(&search.TermQuery{ // The first bucket contains the index rows where the Col_Keyword field is "hangzhou".
					FieldName: "Col_Keyword",
					Term:      "hangzhou",
				}).
				Query(&search.RangeQuery{ // The second bucket contains the index rows where the Col_Long field value is in the range [3, 5].
					FieldName:    "Col_Long",
					From:         3,
					To:           5,
					IncludeLower: true,
					IncludeUpper: true})))

	// Set to return all columns
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
	groupByResults := searchResponse.GroupByResults // Get all aggregation results

	groupByFilterResult, err := groupByResults.GroupByFilter("group_by_filter") // Get the GroupBy result named "group_by_filter", the type is GroupByFilter.
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_filter: ")
	for _, item := range groupByFilterResult.Items { // Iterate through all the returned buckets.
		fmt.Println("\trowCount: ", item.RowCount) // Print the number of rows for this partition bucket
	}
}

func GroupByGeoDistanceSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                                                                              // Match all rows
			SetLimit(100).                                                                                                                  // Limit to returning the first 100 rows of results
			GroupBy(search.NewGroupByGeoDistance("group_by_geo_distance", "Col_GeoPoint", search.GeoPoint{Lat: 30.137817, Lon: 120.08681}). // Perform GroupByGeoDistance geographic range aggregation on the Col_GeoPoint field.
																			Range(search.NegInf, 10000). // The first bucket contains the index rows where the distance between Col_GeoPoint and the center point is (-∞, 10km).
																			Range(10000, 15000).         // The second bucket contains the index rows for Col_GeoPoint that are between 10km and 15km away from the center point.
																			Range(15000, search.Inf)))   // The third bucket contains the index rows where the distance between Col_GeoPoint and the center point is within (15km, +∞).

	// Set to return all columns
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
	groupByResults := searchResponse.GroupByResults // Get all aggregation results

	groupByGeoDistanceResult, err := groupByResults.GroupByGeoDistance("group_by_geo_distance") // Get the GroupBy result named "group_by_geo_distance", the type is GroupByGeoDistance.
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_geo_distance: ")
	for _, item := range groupByGeoDistanceResult.Items { // Iterate through all the returned buckets.
		fmt.Println("\t[", item.From, ", ", item.To, "), rowCount: ", item.RowCount) // Print the number of rows for this partition bucket
	}
}

func GroupByHistogramSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). // Match all rows
			SetLimit(100).                     // Limit to returning the first 100 rows of results
			GroupBy(search.NewGroupByHistogram("group_by_histogram", "Col_Long").
				SetInterval(10).
				SetMinDocCount(1).
				SetFiledRange(0, 100).
				SetMissing(3)))

	// Set to return all columns
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
	groupByResults := searchResponse.GroupByResults // Get all aggregation results

	groupByHistogramResult, err := groupByResults.GroupByHistogram("group_by_histogram") // Get the GroupBy result named "group_by_histogram", the type is GroupByHistogram.
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_histogram: ")
	for _, item := range groupByHistogramResult.Items {
		fmt.Println("key: ", item.Key.Value, ", value: ", item.Value) // Print the returned value
	}
}

func GroupByDateHistogramSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}).                                             // Match all rows
			SetLimit(100).                                                                 // Limit to returning the first 100 rows of results
			GroupBy(search.NewGroupByDateHistogram("group_by_date_histogram", "Col_date"). // Suppose date format is : 'yyyy-MM-dd HH:mm:ss'
													SetInterval(model.DateTimeValue{Unit: model.DateTimeUnit_HOUR.Enum(), Value: proto.Int32(30)}).
													SetMinDocCount(1).
													SetFiledRange("2022-01-01 12:13:14", "2022-01-05 12:13:14").
													SetMissing("2022-01-06 12:13:14")))

	// Set to return all columns
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
	groupByResults := searchResponse.GroupByResults // Get all aggregation results

	groupByDateHistogramResult, err := groupByResults.GroupByDateHistogram("group_by_date_histogram") // Get the GroupBy result named "group_by_date_histogram", the type is GroupByHistogram
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_date_histogram: ")
	for _, item := range groupByDateHistogramResult.Items {
		fmt.Println("timestamp: ", item.Timestamp, ", row_count: ", item.RowCount) // Print the returned value
	}
}

func GroupByGeoGridSample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}

	searchRequest.
		SetTableName(tableName). // Set the table name
		SetIndexName(indexName). // Set the multi-index name
		SetSearchQuery(search.NewSearchQuery().
			SetQuery(&search.MatchAllQuery{}). // Match all rows
			SetLimit(100).                     // Limit to returning the first 100 rows of results
			GroupBy(search.NewGroupByGeoGrid("group_by_geo_grid", "Col_geo").
				SetPrecision(model.GHP_156KM_156KM_3).
				SetSize(10)))

	// Set to return all columns
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
	groupByResults := searchResponse.GroupByResults // Get all aggregation results

	groupByGeoGridResult, err := groupByResults.GroupByGeoGrid("group_by_geo_grid") // Get the GroupBy result named "group_by_geo_grid", the type is GroupByGeoGrid.
	if err != nil {
		panic(err)
	}
	fmt.Println("group_by_geo_grid: ")
	for _, item := range groupByGeoGridResult.Items {
		fmt.Println("key: ", item.Key, ", geoGrid: ", item.GeoGrid, ", rowCount: ", item.RowCount) // Print the returned value
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
 * Single concurrency for ParallelScan
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
 * ParallelScan with multiple concurrency
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
 * Dynamically modify the schema.
 * The index for modifying the schema must end with _reindex.
 */
func UpdateSearchIndexSchema(client *tablestore.TableStoreClient, tableName string, indexName string, indexReindexName string) {
	{
		// Step 1: Create an index
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
		request.TableName = tableName // Set the table name
		request.IndexName = indexName // Set the index name

		schemas := []*tablestore.FieldSchema{}
		field1 := &tablestore.FieldSchema{
			FieldName:        proto.String("Col_Keyword"),  // Set the field name, use proto.String to get a string pointer
			FieldType:        tablestore.FieldType_KEYWORD, // Set the field type
			Index:            proto.Bool(true),             // Set to enable indexing
			EnableSortAndAgg: proto.Bool(true),             // Set to enable the sorting and statistics function
		}
		field2 := &tablestore.FieldSchema{
			FieldName:        proto.String("Col_Long"),
			FieldType:        tablestore.FieldType_LONG,
			Index:            proto.Bool(true),
			EnableSortAndAgg: proto.Bool(true),
		}
		schemas = append(schemas, field1, field2)

		request.IndexSchema = &tablestore.IndexSchema{
			FieldSchemas: schemas, // Set the fields included in the SearchIndex
		}
		resp, err := client.CreateSearchIndex(request) // Call the client to create a SearchIndex
		if err != nil {
			fmt.Println("error :", err)
			return
		}
		fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
	}
	{
		// Step 2. Create the index with the modified schema, removing field2.
		fmt.Println("Begin to create index:", indexReindexName)
		request := &tablestore.CreateSearchIndexRequest{}
		request.TableName = tableName        // Set the table name
		request.IndexName = indexReindexName // Set the index name
		request.SourceIndexName = &indexName // Set the source index: the index whose schema is being modified

		schemas := []*tablestore.FieldSchema{}
		field1 := &tablestore.FieldSchema{
			FieldName:        proto.String("Col_Keyword"),  // Set the field name, use proto.String to get a string pointer.
			FieldType:        tablestore.FieldType_KEYWORD, // Set the field type
			Index:            proto.Bool(true),             // Set to enable indexing
			EnableSortAndAgg: proto.Bool(true),             // Set to enable the sorting and statistics feature
		}
		schemas = append(schemas, field1)

		request.IndexSchema = &tablestore.IndexSchema{
			FieldSchemas: schemas, // Set the fields included in the SearchIndex
		}
		resp, err := client.CreateSearchIndex(request) // Call the client to create a SearchIndex
		if err != nil {
			fmt.Println("error :", err)
			return
		}
		fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
	}
	{
		// Step 3. Set the AB index weight, the weight is between 0-100
		// Before performing this step, you need to wait for the "rebuild index" data synchronization, which goes through two phases: "full synchronization" and "incremental synchronization".
		fmt.Println("wait schema reload")
		time.Sleep(60 * time.Second)
		{
			// The original index weight here is 50, and the new index weight is 50.
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
			// Check if the weight setting is successful
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
			// The original index weight here is 0, and the new index weight is 100.
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
			// Check if the weight setting is successful
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
		// Step 4: Switch the index, at this point the index schema becomes the schema of the new index.
		switchReq := new(tablestore.UpdateSearchIndexRequest)
		switchReq.TableName = tableName
		switchReq.IndexName = indexName
		switchReq.SwitchIndexName = &indexReindexName
		resp, err := client.UpdateSearchIndex(switchReq)
		if err != nil {
			fmt.Println("update search index failed with error:", err)
		}
		fmt.Println("UpdateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
		// Check if the schema changes to the new one after the index switch is completed.
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

		// If an issue is found, there is still a chance to switch back.
		//switchReq := new(tablestore.UpdateSearchIndexRequest)
		//switchReq.TableName = tableName
		//switchReq.IndexName = indexName
		//switchReq.SwitchIndexName = indexReindexName
		//resp, err := client.UpdateSearchIndex(switchReq)
	}
	{
		// Step 5. After a period of silence, the index before modification can be deleted.
		DeleteSearchIndex(client, tableName, indexReindexName)
	}
}

func CreateSearchIndexWithJsonField(client *tablestore.TableStoreClient, tableName string, indexName string) {
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
	request.TableName = tableName // Set the table name
	request.IndexName = indexName // Set the index name

	var schemas []*tablestore.FieldSchema
	field := &tablestore.FieldSchema{ // Do not set IsArray for Json Field
		FieldName: proto.String("Col_ObjectJson"),    // Set the field name, use proto.String to get a string pointer
		FieldType: tablestore.FieldType_JSON,         // Set the field type
		JsonType:  tablestore.JsonType_OBJECT.Enum(), // Set the json type, use JsonType_XXX.Enum()
		FieldSchemas: []*tablestore.FieldSchema{
			{
				FieldName: proto.String("Col_Ip"),
				FieldType: tablestore.FieldType_IP,
				Index:     proto.Bool(true),
			},
			{
				FieldName: proto.String("Col_NestedJson"),
				FieldType: tablestore.FieldType_JSON,
				JsonType:  tablestore.JsonType_NESTED.Enum(),
				FieldSchemas: []*tablestore.FieldSchema{
					{
						FieldName: proto.String("Col_NestedJson_Long"),
						FieldType: tablestore.FieldType_LONG,
					},
					{
						FieldName: proto.String("Col_NestedJson_Keyword"),
						FieldType: tablestore.FieldType_KEYWORD,
					},
				},
			},
		},
	}

	schemas = append(schemas, field)

	request.IndexSchema = &tablestore.IndexSchema{
		FieldSchemas: schemas, // Set the fields included in the SearchIndex
	}
	resp, err := client.CreateSearchIndex(request) // Call the client to create a SearchIndex
	if err != nil {
		fmt.Println("error :", err)
		return
	}
	fmt.Println("CreateSearchIndex finished, requestId:", resp.ResponseInfo.RequestId)
}

func WriteDataForJsonField(client *tablestore.TableStoreClient, tableName string) {
	fmt.Println("Begin to write data")

	for i := 0; i < 100; i++ {
		putRowRequest := new(tablestore.PutRowRequest)
		putRowChange := new(tablestore.PutRowChange)
		putRowChange.TableName = tableName
		putPk := new(tablestore.PrimaryKey)
		putPk.AddPrimaryKeyColumn("pk1", fmt.Sprintf("pk_val%d", i))

		putRowChange.PrimaryKey = putPk
		putRowChange.AddColumn("Col_ObjectJson", fmt.Sprintf(`{"Col_Ip":"192.168.1.%d","Col_NestedJson":{"Col_NestedJson_Long":%d,"Col_NestedJson_Keyword":"keyword_%d"}}`, i, i, i))
		putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
		putRowRequest.PutRowChange = putRowChange
		_, err := client.PutRow(putRowRequest)

		if err != nil {
			fmt.Println("putrow failed with error:", err)
		}
	}

	time.Sleep(30 * time.Second)
}

func JsonQuerySample(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.TermQuery{}              // Set the query type to MatchQuery
	query.FieldName = "Col_ObjectJson.Col_Ip" // Set the subfield in Json, use `.` to join the field name and subfield name
	query.Term = "192.168.1.1"                // Set the value to match
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetOffset(0) // Set the offset to 0
	searchQuery.SetLimit(20) // Set the limit to 20, which means a maximum of 20 data entries will be returned.
	searchRequest.SetSearchQuery(searchQuery)
	// Set to return all columns
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAllFromIndex: true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}

	// search nested type json in json field
	nestedQuery := &search.NestedQuery{
		Path:      "Col_ObjectJson.Col_NestedJson", // the path of nested field
		ScoreMode: search.ScoreMode_Avg,
		Query: &search.RangeQuery{
			FieldName:    "Col_ObjectJson.Col_NestedJson.Col_NestedJson_Long",
			From:         1,
			To:           20,
			IncludeLower: true,
			IncludeUpper: true,
		},
	}
	searchQuery.SetQuery(nestedQuery)
	searchRequest.SetSearchQuery(searchQuery)
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // Check if the returned result is complete
	fmt.Println("RowCount: ", len(searchResponse.Rows))
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody))
	}
}
