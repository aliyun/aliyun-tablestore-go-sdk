package search

import (
	"encoding/json"
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
)

// Common Case
var disMax = &DisMaxQuery{
	Queries: []Query{
		&MatchQuery{
			FieldName: "name",
			Text:      "张三",
		},
		&MatchQuery{
			FieldName: "name",
			Text:      "李四",
		},
	},
	TieBreaker: Float32Ptr(0.5),
	Weight:     Float32Ptr(1.5),
}

const jsonString = `{"TieBreaker":0.5,"Weight":1.5,"Queries":[{"Name":"MatchQuery","Query":{"FieldName":"name","Text":"张三","MinimumShouldMatch":null,"Operator":null,"Weight":null,"MinShouldMatch":null}},{"Name":"MatchQuery","Query":{"FieldName":"name","Text":"李四","MinimumShouldMatch":null,"Operator":null,"Weight":null,"MinShouldMatch":null}}]}`

func TestDisMaxQuery_MarshalJSON(t *testing.T) {
	// Test normal serialization
	marshaled, err := json.Marshal(disMax)
	assert.Nil(t, err)
	assert.Equal(t, jsonString, string(marshaled))

	// Test serialization with empty query list
	emptyDisMax := &DisMaxQuery{
		TieBreaker: Float32Ptr(0.2),
		Weight:     Float32Ptr(1.0),
	}
	emptyMarshaled, err := json.Marshal(emptyDisMax)
	assert.Nil(t, err)
	expectedEmpty := `{"TieBreaker":0.2,"Weight":1,"Queries":null}`
	assert.Equal(t, expectedEmpty, string(emptyMarshaled))

	// Test case with only TieBreaker
	tieBreakerOnly := &DisMaxQuery{
		Queries: []Query{
			&MatchQuery{
				FieldName: "title",
				Text:      "test",
			},
		},
		TieBreaker: Float32Ptr(0.7),
	}
	tieBreakerOnlyMarshaled, err := json.Marshal(tieBreakerOnly)
	assert.Nil(t, err)
	expectedTieBreakerOnly := `{"TieBreaker":0.7,"Weight":null,"Queries":[{"Name":"MatchQuery","Query":{"FieldName":"title","Text":"test","MinimumShouldMatch":null,"Operator":null,"Weight":null,"MinShouldMatch":null}}]}`
	assert.Equal(t, expectedTieBreakerOnly, string(tieBreakerOnlyMarshaled))

	// Test case with only Weight
	weightOnly := &DisMaxQuery{
		Queries: []Query{
			&MatchQuery{
				FieldName: "title",
				Text:      "test",
			},
		},
		Weight: Float32Ptr(2.0),
	}
	weightOnlyMarshaled, err := json.Marshal(weightOnly)
	assert.Nil(t, err)
	expectedWeightOnly := `{"TieBreaker":null,"Weight":2,"Queries":[{"Name":"MatchQuery","Query":{"FieldName":"title","Text":"test","MinimumShouldMatch":null,"Operator":null,"Weight":null,"MinShouldMatch":null}}]}`
	assert.Equal(t, expectedWeightOnly, string(weightOnlyMarshaled))
}

func TestDisMaxQuery_UnmarshalJSON(t *testing.T) {
	// Test normal deserialization
	disMaxQuery := &DisMaxQuery{}
	err := json.Unmarshal([]byte(jsonString), disMaxQuery)
	assert.Nil(t, err)

	// Verify basic fields
	assert.Equal(t, float32(0.5), *disMaxQuery.TieBreaker)
	assert.Equal(t, float32(1.5), *disMaxQuery.Weight)

	// Verify query array
	assert.Equal(t, 2, len(disMaxQuery.Queries))

	// Verify first query
	matchQuery1, ok := disMaxQuery.Queries[0].(*MatchQuery)
	assert.True(t, ok)
	assert.Equal(t, "name", matchQuery1.FieldName)
	assert.Equal(t, "张三", matchQuery1.Text)

	// Verify second query
	matchQuery2, ok := disMaxQuery.Queries[1].(*MatchQuery)
	assert.True(t, ok)
	assert.Equal(t, "name", matchQuery2.FieldName)
	assert.Equal(t, "李四", matchQuery2.Text)

	// Test deserialization with empty query list
	emptyJson := `{"TieBreaker":0.2,"Weight":1,"Queries":[]}`
	emptyDisMax := &DisMaxQuery{}
	err = json.Unmarshal([]byte(emptyJson), emptyDisMax)
	assert.Nil(t, err)
	assert.Equal(t, float32(0.2), *emptyDisMax.TieBreaker)
	assert.Equal(t, float32(1.0), *emptyDisMax.Weight)
	assert.Equal(t, 0, len(emptyDisMax.Queries))

	// Test deserialization with only TieBreaker
	tieBreakerOnlyJson := `{"TieBreaker":0.7,"Weight":null,"Queries":[{"Name":"MatchQuery","Query":{"FieldName":"title","Text":"test","MinimumShouldMatch":null,"Operator":null}}]}`
	tieBreakerOnly := &DisMaxQuery{}
	err = json.Unmarshal([]byte(tieBreakerOnlyJson), tieBreakerOnly)
	assert.Nil(t, err)
	assert.Equal(t, float32(0.7), *tieBreakerOnly.TieBreaker)
	assert.Nil(t, tieBreakerOnly.Weight)
	assert.Equal(t, 1, len(tieBreakerOnly.Queries))

	// Test deserialization with only Weight
	weightOnlyJson := `{"TieBreaker":null,"Weight":2,"Queries":[{"Name":"MatchQuery","Query":{"FieldName":"title","Text":"test","MinimumShouldMatch":null,"Operator":null}}]}`
	weightOnly := &DisMaxQuery{}
	err = json.Unmarshal([]byte(weightOnlyJson), weightOnly)
	assert.Nil(t, err)
	assert.Nil(t, weightOnly.TieBreaker)
	assert.Equal(t, float32(2.0), *weightOnly.Weight)
	assert.Equal(t, 1, len(weightOnly.Queries))

	// Test invalid JSON
	invalidJson := `{"invalid": json}`
	invalidDisMax := &DisMaxQuery{}
	err = json.Unmarshal([]byte(invalidJson), invalidDisMax)
	assert.NotNil(t, err)
}

func TestDisMaxQuery_Type(t *testing.T) {
	// Test Type method
	assert.Equal(t, QueryType_DisMaxQuery, disMax.Type())
}

func TestDisMaxQuery_Serialize(t *testing.T) {
	// Test normal serialization
	data, err := disMax.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test serialization with empty query list
	emptyDisMax := &DisMaxQuery{
		TieBreaker: Float32Ptr(0.2),
		Weight:     Float32Ptr(1.0),
	}
	data, err = emptyDisMax.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test serialization with invalid query
	invalidDisMax := &DisMaxQuery{
		Queries: []Query{
			&MockInvalidQuery{}, // Assuming this is a query that returns an error
		},
	}
	// This test requires MockInvalidQuery definition to run
	data, err = invalidDisMax.Serialize()
	assert.NotNil(t, err)
	assert.Nil(t, data)
}

func TestDisMaxQuery_ProtoBuffer(t *testing.T) {
	// Test ProtoBuffer method
	pbQuery, err := disMax.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, *pbQuery.Type, otsprotocol.QueryType_DIS_MAX_QUERY)

	// Test with empty query list
	emptyDisMax := &DisMaxQuery{
		TieBreaker: Float32Ptr(0.2),
		Weight:     Float32Ptr(1.0),
	}
	pbQuery, err = emptyDisMax.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, *pbQuery.Type, otsprotocol.QueryType_DIS_MAX_QUERY)
}

// MockInvalidQuery is a mock query for testing error cases
type MockInvalidQuery struct{}

func (q *MockInvalidQuery) Type() QueryType {
	return QueryType_MatchQuery
}

func (q *MockInvalidQuery) Serialize() ([]byte, error) {
	// Simulate serialization error
	return nil, &MockError{"serialize error"}
}

func (q *MockInvalidQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	// Simulate ProtoBuffer error
	return nil, &MockError{"protobuf error"}
}

type MockError struct {
	msg string
}

func (e *MockError) Error() string {
	return e.msg
}

func TestDisMaxQuery_Serialize_Error(t *testing.T) {
	// Test error handling when containing invalid query
	invalidDisMax := &DisMaxQuery{
		Queries: []Query{
			&MockInvalidQuery{},
		},
		TieBreaker: Float32Ptr(0.6),
		Weight:     Float32Ptr(1.0),
	}

	data, err := invalidDisMax.Serialize()
	assert.NotNil(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "protobuf error")
}
