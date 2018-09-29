package sample

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"encoding/json"
	"github.com/golang/protobuf/proto"
)

/**
 *创建一个SearchIndex，包含Col_Keyword和Col_Long两列，类型分别设置为字符串(KEYWORD)和整型(LONG)。
 */
func CreateSearchIndex(client *tablestore.TableStoreClient, tableName string, indexName string) {
	request := &tablestore.CreateSearchIndexRequest{}
	request.TableName = tableName // 设置表名
	request.IndexName = indexName // 设置索引名

	schemas := []*tablestore.FieldSchema{}
	field1 := &tablestore.FieldSchema{
		FieldName: proto.String("Col_Keyword"), // 设置字段名，使用proto.String用于获取字符串指针
		FieldType: tablestore.FieldType_KEYWORD, // 设置字段类型
		Index:     proto.Bool(true), // 设置开启索引
		EnableSortAndAgg: proto.Bool(true), // 设置开启排序与统计功能
	}
	field2 := &tablestore.FieldSchema{
		FieldName: proto.String("Col_Long"),
		FieldType: tablestore.FieldType_LONG,
		Index:     proto.Bool(true),
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

func MatchAllQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.MatchAllQuery{}
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchQuery.SetLimit(0)
	searchRequest.SetSearchQuery(searchQuery)
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess)
	fmt.Println("TotalCount: ", searchResponse.TotalCount)
}

/**
 *  查询表中Col_Keyword这一列的值能够匹配"hangzhou"的数据，返回匹配到的总行数和一些匹配成功的行。
 */
func MatchQuery(client *tablestore.TableStoreClient, tableName string, indexName string) {
	searchRequest := &tablestore.SearchRequest{}
	searchRequest.SetTableName(tableName)
	searchRequest.SetIndexName(indexName)
	query := &search.MatchQuery{} // 设置查询类型为MatchQuery
	query.FieldName = "Col_Keyword" // 设置要匹配的字段
	query.Text = "hangzhou" // 设置要匹配的值
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
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
	fmt.Println("RowCount: ", len(searchResponse.Rows)) // 返回的行数
	for _, row := range searchResponse.Rows {
		jsonBody, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}
		fmt.Println("Row: ", string(jsonBody)) // 不设置columnsToGet，默认只返回主键
	}
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:true,
	})
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
	query.FieldName = "Col_Text"  // 设置要匹配的字段
	query.Text = "hangzhou shanghai"  // 设置要匹配的值
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
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
		ReturnAll:true,
	})
	searchResponse, err = client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
	query := &search.TermQuery{} // 设置查询类型为TermQuery
	query.FieldName = "Col_Keyword" // 设置要匹配的字段
	query.Term = "hangzhou" // 设置要匹配的值
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
	query := &search.PrefixQuery{} // 设置查询类型为PrefixQuery
	query.FieldName = "Col_Keyword" // 设置要匹配的字段
	query.Prefix = "hangzhou" // 设置前缀
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
		ReturnAll:true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
	rangeQuery.FieldName = "Col_Long" // 设置针对哪个字段
	rangeQuery.GT(3) // 设置该字段的范围条件，大于3
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
		ReturnAll:true,
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
	query.FieldName = "Col_GeoPoint"  // 设置比较哪个字段的值
	query.TopLeft = "10,0"  // 设置矩形左上角
	query.BottomRight = "0,10" // 设置矩形右下角
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
	query.CenterPoint = "5,5"  // 设置中心点
	query.DistanceInMeter = 10000.0 // 设置到中心点的距离条件，不超过10000米
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
	query.Points = []string{"0,0","5,5","5,0"} // 设置多边形的顶点
	searchQuery := search.NewSearchQuery()
	searchQuery.SetQuery(query)
	searchRequest.SetSearchQuery(searchQuery)
	// 设置返回所有列
	searchRequest.SetColumnsToGet(&tablestore.ColumnsToGet{
		ReturnAll:true,
	})
	searchResponse, err := client.Search(searchRequest)
	if err != nil {
		fmt.Printf("%#v", err)
		return
	}
	fmt.Println("IsAllSuccess: ", searchResponse.IsAllSuccess) // 查看返回结果是否完整
	fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
		fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
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
		fmt.Println("TotalCount: ", searchResponse.TotalCount) // 匹配的总行数
		fmt.Println("RowCount: ", len(searchResponse.Rows))
	}
}
