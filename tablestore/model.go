package tablestore

import (
	"net/http"
	"time"
	"github.com/golang/protobuf/proto"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/tsprotocol"
	"math/rand"
)

// @class OTSClient
// The OTSClient, which will connect OTS service for authorization, create/list/
// delete tables/table groups, to get/put/delete a row.
// Note: OTSClient is NOT thread-safe.
// OTSClient的功能包括连接OTS服务进行验证、创建/列出/删除表或表组、插入/获取/
// 删除/更新行数据
type (
	TableStoreClient struct {
		endPoint        string
		instanceName    string
		accessKeyId     string
		accessKeySecret string
		securityToken   string

		httpClient      IHttpClient
		config          *TableStoreConfig
		random          *rand.Rand
	}
	ClientOption func(*TableStoreClient)
)

type TableStoreHttpClient struct {
	httpClient      *http.Client
}

// use this to mock http.client for testing
type IHttpClient interface {
	Do(*http.Request) (*http.Response, error)
	New(*http.Client)
}

func (httpClient *TableStoreHttpClient) Do(req *http.Request) (*http.Response, error) {
	return httpClient.httpClient.Do(req)
}

func (httpClient *TableStoreHttpClient) New(client *http.Client) {
	httpClient.httpClient = client
}

type HTTPTimeout struct {
	ConnectionTimeout time.Duration
	RequestTimeout    time.Duration
}

type TableStoreConfig struct {
	RetryTimes  uint
	MaxRetryTime time.Duration
	HTTPTimeout HTTPTimeout
}

type CreateTableRequest struct {
	TableMeta          *TableMeta
	TableOption        *TableOption
	ReservedThroughput *ReservedThroughput
}

type CreateTableResponse struct {

}

type DeleteTableResponse struct {

}

type TableMeta struct {
	TableName   string
	SchemaEntry []*PrimaryKeySchema
}

type PrimaryKeySchema struct {
	Name   *string
	Type   *PrimaryKeyType
	Option *PrimaryKeyOption
}

type PrimaryKey struct {
	PrimaryKeys []*PrimaryKeyColumn
}

type TableOption struct {
	TimeToAlive, MaxVersion int
}

type ReservedThroughput struct {
	Readcap, Writecap int
}

type ListTableResponse struct {
	TableNames []string
}

type DeleteTableRequest struct {
	TableName string
}

type DescribeTableRequest struct {
	TableName string
}

type DescribeTableResponse struct {
	TableMeta          *TableMeta
	TableOption        *TableOption
	ReservedThroughput *ReservedThroughput
}

type UpdateTableRequest struct {
	TableName          string
	TableOption        *TableOption
	ReservedThroughput *ReservedThroughput
}

type UpdateTableResponse struct {
	TableOption        *TableOption
	ReservedThroughput *ReservedThroughput
}

type ConsumedCapacityUnit struct {
	Read  int32
	Write int32
}

type PutRowResponse struct {
	ConsumedCapacityUnit *ConsumedCapacityUnit
}

type DeleteRowResponse struct {
	ConsumedCapacityUnit *ConsumedCapacityUnit
}

type UpdateRowResponse struct {
	ConsumedCapacityUnit *ConsumedCapacityUnit
}

type PrimaryKeyType int32

const (
	PrimaryKeyType_INTEGER PrimaryKeyType = 1
	PrimaryKeyType_STRING PrimaryKeyType = 2
	PrimaryKeyType_BINARY PrimaryKeyType = 3
)

const (
	DefaultRetryInterval = 10
	MaxRetryInterval = 320
)

type PrimaryKeyOption int32

const (
	NONE           PrimaryKeyOption = 0
	AUTO_INCREMENT PrimaryKeyOption = 1
	MIN            PrimaryKeyOption = 2
	MAX            PrimaryKeyOption = 3
)

type PrimaryKeyColumn struct {
	ColumnName string
	Value      interface{}
	PrimaryKeyOption PrimaryKeyOption
}

type AttributeColumn struct {
	ColumnName string
	Value      interface{}
	Timestamp    int64
}

type TimeRange struct {
	Start    int64
	End      int64
	Specific int64
}

type ColumnToUpdate struct {
	ColumnName   string
	Type         byte
	Timestamp    int64
	HasType      bool
	HasTimestamp bool
	IgnoreValue  bool
	Value        interface{}
}

type RowExistenceExpectation int

const (
	RowExistenceExpectation_IGNORE RowExistenceExpectation = 0
	RowExistenceExpectation_EXPECT_EXIST RowExistenceExpectation = 1
	RowExistenceExpectation_EXPECT_NOT_EXIST RowExistenceExpectation = 2
)

type ComparatorType int32

const (
	CT_EQUAL ComparatorType = 1
	CT_NOT_EQUAL ComparatorType = 2
	CT_GREATER_THAN ComparatorType = 3
	CT_GREATER_EQUAL ComparatorType = 4
	CT_LESS_THAN ComparatorType = 5
	CT_LESS_EQUAL ComparatorType = 6
)

type LogicalOperator int32

const (
	LO_NOT LogicalOperator = 1
	LO_AND LogicalOperator = 2
	LO_OR LogicalOperator = 3
)

type FilterType int32

const (
	FT_SINGLE_COLUMN_VALUE FilterType = 1
	FT_COMPOSITE_COLUMN_VALUE FilterType = 2
	FT_COLUMN_PAGINATION FilterType = 3
)

type ColumnFilter interface {
	Serialize() []byte
	ToFilter() *tsprotocol.Filter
}

type SingleColumnCondition struct {
	Comparator        *ComparatorType
	ColumnName        *string
	ColumnValue       interface{} //[]byte
	FilterIfMissing   bool
	LatestVersionOnly bool
}

type PaginationFilter struct {
	Offset int32
	Limit  int32
}

type CompositeColumnValueFilter struct {
	Operator LogicalOperator
	Filters  []ColumnFilter
}

func (ccvfilter *CompositeColumnValueFilter) Serialize() []byte {
	result, _ := proto.Marshal(ccvfilter.ToFilter())
	return result
}

func (ccvfilter *CompositeColumnValueFilter) ToFilter() *tsprotocol.Filter {
	compositefilter := NewCompositeFilter(ccvfilter.Filters, ccvfilter.Operator)
	compositeFilterToBytes, _ := proto.Marshal(compositefilter)
	filter := new(tsprotocol.Filter)
	filter.Type = tsprotocol.FilterType_FT_COMPOSITE_COLUMN_VALUE.Enum()
	filter.Filter = compositeFilterToBytes
	return filter
}

func (ccvfilter *CompositeColumnValueFilter) AddFilter(filter ColumnFilter) {
	ccvfilter.Filters = append(ccvfilter.Filters, filter)
}

func (condition *SingleColumnCondition) ToFilter() *tsprotocol.Filter {
	singlefilter := NewSingleColumnValueFilter(condition)
	singleFilterToBytes, _ := proto.Marshal(singlefilter)
	filter := new(tsprotocol.Filter)
	filter.Type = tsprotocol.FilterType_FT_SINGLE_COLUMN_VALUE.Enum()
	filter.Filter = singleFilterToBytes
	return filter
}

func (condition *SingleColumnCondition) Serialize() []byte {
	result, _ := proto.Marshal(condition.ToFilter())
	return result
}

func (pageFilter *PaginationFilter) ToFilter() *tsprotocol.Filter {
	compositefilter := NewPaginationFilter(pageFilter)
	compositeFilterToBytes, _ := proto.Marshal(compositefilter)
	filter := new(tsprotocol.Filter)
	filter.Type = tsprotocol.FilterType_FT_COLUMN_PAGINATION.Enum()
	filter.Filter = compositeFilterToBytes
	return filter
}

func (pageFilter *PaginationFilter) Serialize() []byte {
	result, _ := proto.Marshal(pageFilter.ToFilter())
	return result
}

type RowCondition struct {
	RowExistenceExpectation RowExistenceExpectation
	ColumnCondition         ColumnFilter
}

type PutRowChange struct {
	TableName  string
	PrimaryKey *PrimaryKey
	Columns    []AttributeColumn
	Condition  *RowCondition
}

type PutRowRequest struct {
	PutRowChange *PutRowChange
}

type DeleteRowChange struct {
	TableName  string
	PrimaryKey *PrimaryKey
	Condition  *RowCondition
}

type DeleteRowRequest struct {
	DeleteRowChange *DeleteRowChange
}

type SingleRowQueryCriteria struct {
	ColumnsToGet []string
	TableName    string
	PrimaryKey   *PrimaryKey
	MaxVersion   int32
	TimeRange    *TimeRange
	Filter       ColumnFilter
}

type UpdateRowChange struct {
	TableName  string
	PrimaryKey *PrimaryKey
	Columns    []ColumnToUpdate
	Condition  *RowCondition
}

type UpdateRowRequest struct {
	UpdateRowChange *UpdateRowChange
}

func (rowQueryCriteria *SingleRowQueryCriteria) AddColumnToGet(columnName string) {
	rowQueryCriteria.ColumnsToGet = append(rowQueryCriteria.ColumnsToGet, columnName)
}

func (rowQueryCriteria *SingleRowQueryCriteria) getColumnsToGet() []string {
	return rowQueryCriteria.ColumnsToGet
}

func (rowQueryCriteria *MultiRowQueryCriteria) AddColumnToGet(columnName string) {
	rowQueryCriteria.ColumnsToGet = append(rowQueryCriteria.ColumnsToGet, columnName)
}

func (rowQueryCriteria *RangeRowQueryCriteria) AddColumnToGet(columnName string) {
	rowQueryCriteria.ColumnsToGet = append(rowQueryCriteria.ColumnsToGet, columnName)
}

func (rowQueryCriteria *MultiRowQueryCriteria) AddRow(pk *PrimaryKey) {
	rowQueryCriteria.PrimaryKey = append(rowQueryCriteria.PrimaryKey, pk)
}

type GetRowRequest struct {
	SingleRowQueryCriteria *SingleRowQueryCriteria
}

type MultiRowQueryCriteria struct {
	PrimaryKey   []*PrimaryKey
	ColumnsToGet []string
	TableName    string
	MaxVersion   int
	TimeRange    *TimeRange
	Filter       ColumnFilter
}

type BatchGetRowRequest struct {
	MultiRowQueryCriteria []*MultiRowQueryCriteria
}

type ColumnMap struct {
	Columns map[string][]*AttributeColumn
	columnsKey []string
}

type GetRowResponse struct {
	PrimaryKey           PrimaryKey
	Columns              []*AttributeColumn
	ConsumedCapacityUnit *ConsumedCapacityUnit
	columnMap            *ColumnMap
}

type Error struct {
	Code string
	Message string
}

type RowResult struct {
	TableName string
	IsSucceed bool
	Error Error
	PrimaryKey PrimaryKey
	Columns    []*AttributeColumn
	ConsumedCapacityUnit *ConsumedCapacityUnit
	Index int32
}

type RowChange interface {
	Serialize() []byte
	getOperationType() tsprotocol.OperationType
	getCondition() *tsprotocol.Condition
	GetTableName() string
}

type BatchGetRowResponse struct {
	TableToRowsResult map[string][]RowResult
}

type BatchWriteRowRequest struct{
	RowChangesGroupByTable map[string][]RowChange
}

type BatchWriteRowResponse struct{
	TableToRowsResult map[string][]RowResult
}

type Direction int32

const (
	FORWARD           Direction = 0
	BACKWARD          Direction = 1
)

type RangeRowQueryCriteria struct{
	TableName    string
	StartPrimaryKey *PrimaryKey
	EndPrimaryKey *PrimaryKey
	ColumnsToGet []string
	MaxVersion   int32
	TimeRange    *TimeRange
	Filter       ColumnFilter
	Direction    Direction
	Limit        int32
}

type GetRangeRequest struct {
	RangeRowQueryCriteria *RangeRowQueryCriteria
}

type Row struct{
	PrimaryKey           *PrimaryKey
	Columns              []*AttributeColumn
}

type GetRangeResponse struct {
	Rows []*Row
	ConsumedCapacityUnit *ConsumedCapacityUnit
	NextStartPrimaryKey *PrimaryKey
}