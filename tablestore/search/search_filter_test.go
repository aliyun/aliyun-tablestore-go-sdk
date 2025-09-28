package search

import (
	"bytes"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

type MockQuery struct {
	mock.Mock
}

func (m *MockQuery) Type() QueryType {
	return QueryType_MatchQuery
}

func (m *MockQuery) Serialize() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	args := m.Called()
	return args.Get(0).(*otsprotocol.Query), args.Error(1)
}

func TestSearchFilter(t *testing.T) {
	// not nil
	filter := &SearchFilter{}
	query := &MatchQuery{FieldName: "col1", Text: "hello"}
	filter.SetQuery(query)
	filterPb, err := filter.ProtoBuffer()
	if err != nil {
		t.Fatal("build filter pb failed: ", err)
	}
	if filterPb.GetQuery().GetType() != otsprotocol.QueryType_MATCH_QUERY {
		t.Fatalf("build filter pb failed, inner queryType expected:%v, actual:%v", otsprotocol.QueryType_MATCH_QUERY, filterPb.GetQuery().GetType())
	}

	queryInner := &otsprotocol.MatchQuery{}
	queryInner.FieldName = &query.FieldName
	queryInner.Text = &query.Text
	queryInnerSerialized, _ := proto.Marshal(queryInner)

	if bytes.Compare(filterPb.GetQuery().GetQuery(), queryInnerSerialized) != 0 {
		t.Fatalf("build filter pb failed, expected:%v, actual:%v", queryInnerSerialized, filterPb.GetQuery().GetQuery())
	}

	// nil
	filter = &SearchFilter{}
	filterPb, err = filter.ProtoBuffer()
	if err != nil {
		t.Fatal("build filter pb failed: ", err)
	}
	if filterPb.GetQuery() != nil {
		t.Fatalf("build filter pb failed, expected:nil, actual:%v", filterPb.GetQuery())
	}

	// err when .ProtoBuffer()
	filter = &SearchFilter{}
	mockQuery := &MockQuery{}
	mockQuery.On("ProtoBuffer").Return(&otsprotocol.Query{}, assert.AnError)
	filter.SetQuery(mockQuery)
	filterPb, err = filter.ProtoBuffer()
	assert.Equal(t, assert.AnError, err)
}

func TestFilterJSON(t *testing.T) {
	// json success
	{
		filter := &SearchFilter{}
		query := MatchQuery{FieldName: "col1", Text: "hello"}
		filter.SetQuery(&query)
		if jsonData, err := filter.MarshalJSON(); err != nil {
			t.Fatal("filter marshal json failed:", err)
		} else {
			newFilter := &SearchFilter{}
			if err := newFilter.UnmarshalJSON(jsonData); err != nil {
				t.Fatal("filter unmarshal json failed:", err)
			}
			assert.NotNil(t, newFilter)
			assert.Equal(t, filter, newFilter)
		}
	}
	// json err
	{
		filter := &SearchFilter{}
		// this json is not complete
		jsonStr := `"SearchFilter":{"Query":{"Name":"MatchQuery","Query":{"FieldName":"col1","Text":"hello","MinimumShouldMatch":null,"Operator":null}`
		jsonData := []byte(jsonStr)
		err := filter.UnmarshalJSON(jsonData)
		assert.NotNil(t, err)
	}

	// from json string
	{
		jsonStr := `{"Offset":-1,"Limit":10,"Collapse":null,"Sort":null,"GetTotalCount":true,"Token":null,"Query":{"Name":"BoolQuery","Query":{"MinimumShouldMatch":null,"MustQueries":null,"MustNotQueries":null,"FilterQueries":null,"ShouldQueries":[{"Name":"RangeQuery","Query":{"FieldName":"gid","From":null,"To":10,"IncludeLower":false,"IncludeUpper":false}},{"Name":"TermQuery","Query":{"FieldName":"gid","Term":"77"}}]}},"SearchFilter":{"Query":{"Name":"MatchQuery","Query":{"FieldName":"col1","Text":"hello","MinimumShouldMatch":null,"Operator":null}}},"Aggregations":[{"Name":"avg","Aggregation":{"AggName":"agg1","Field":"gid","MissingValue":null}}]}`
		jsonData := []byte(jsonStr)
		searchQuery := NewSearchQuery()
		err := searchQuery.UnmarshalJSON(jsonData)
		assert.Nil(t, err)
		assert.Equal(t, searchQuery.SearchFilter, &SearchFilter{Query: &MatchQuery{FieldName: "col1", Text: "hello"}})
	}
}

func TestSearchFilterSerialize(t *testing.T) {
	// full SearchQuery serialize success
	{
		query := &MatchQuery{FieldName: "col1", Text: "hello"}
		sq := &searchQuery{}
		sq.SetSearchFilter(&SearchFilter{Query: query})
		serialized, err := sq.Serialize()
		assert.Nil(t, err)
		assert.NotZero(t, len(serialized))
	}
	// err
	{
		sq := &searchQuery{}
		mockQuery := &MockQuery{}
		mockQuery.On("ProtoBuffer").Return(&otsprotocol.Query{}, assert.AnError)
		sq.SetSearchFilter(&SearchFilter{Query: mockQuery})
		_, err := sq.Serialize()
		assert.Equal(t, assert.AnError, err)
	}
}
