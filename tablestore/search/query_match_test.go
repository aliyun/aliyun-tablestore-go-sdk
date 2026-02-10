package search

import (
	"encoding/json"
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestQueryOperator_Enum(t *testing.T) {
	// Test QueryOperator_OR Enum
	operatorOr := QueryOperator_OR
	enumOr := operatorOr.Enum()
	assert.Equal(t, QueryOperator_OR, *enumOr)

	// Test QueryOperator_AND Enum
	operatorAnd := QueryOperator_AND
	enumAnd := operatorAnd.Enum()
	assert.Equal(t, QueryOperator_AND, *enumAnd)
}

func TestQueryOperator_ProtoBuffer(t *testing.T) {
	// Test QueryOperator_OR ProtoBuffer conversion
	operatorOr := QueryOperator_OR.Enum()
	pbOperatorOr, err := operatorOr.ProtoBuffer()
	assert.Nil(t, err)
	assert.Equal(t, otsprotocol.QueryOperator_OR, *pbOperatorOr)

	// Test QueryOperator_AND ProtoBuffer conversion
	operatorAnd := QueryOperator_AND.Enum()
	pbOperatorAnd, err := operatorAnd.ProtoBuffer()
	assert.Nil(t, err)
	assert.Equal(t, otsprotocol.QueryOperator_AND, *pbOperatorAnd)

	// Test nil QueryOperator ProtoBuffer conversion
	var nilOperator *QueryOperator
	pbNilOperator, err := nilOperator.ProtoBuffer()
	assert.NotNil(t, err)
	assert.Nil(t, pbNilOperator)
	assert.Equal(t, "query operator is nil", err.Error())

	// Test unknown QueryOperator ProtoBuffer conversion
	unknownOperatorValue := QueryOperator(99)
	unknownOperator := &unknownOperatorValue
	pbUnknownOperator, err := unknownOperator.ProtoBuffer()
	assert.NotNil(t, err)
	assert.Nil(t, pbUnknownOperator)
	assert.Contains(t, err.Error(), "unknown query operator")
}

func TestMatchQuery_Type(t *testing.T) {
	// Test MatchQuery Type method
	matchQuery := &MatchQuery{
		FieldName: "title",
		Text:      "test",
	}
	queryType := matchQuery.Type()
	assert.Equal(t, QueryType_MatchQuery, queryType)
}

func TestMatchQuery_Serialize(t *testing.T) {
	// Test basic MatchQuery serialization
	matchQuery := &MatchQuery{
		FieldName: "title",
		Text:      "search text",
	}
	data, err := matchQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test MatchQuery serialization with all fields
	minShouldMatchValue := int32(2)
	operator := QueryOperator_AND
	weight := float32(1.5)
	matchQueryFull := &MatchQuery{
		FieldName:          "content",
		Text:               "full query",
		MinimumShouldMatch: &minShouldMatchValue,
		Operator:           &operator,
		Weight:             &weight,
		MinShouldMatch:     proto.String("2"),
	}
	dataFull, err := matchQueryFull.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, dataFull)

	// Test MatchQuery serialization with nil Operator
	matchQueryNilOperator := &MatchQuery{
		FieldName: "title",
		Text:      "test",
		Operator:  nil,
	}
	dataNilOperator, err := matchQueryNilOperator.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, dataNilOperator)

	// Test MatchQuery serialization with invalid Operator
	invalidOperatorValue := QueryOperator(99)
	matchQueryInvalidOperator := &MatchQuery{
		FieldName: "title",
		Text:      "test",
		Operator:  &invalidOperatorValue,
	}
	dataInvalidOperator, err := matchQueryInvalidOperator.Serialize()
	assert.NotNil(t, err)
	assert.Nil(t, dataInvalidOperator)
	assert.Contains(t, err.Error(), "unknown query operator")
}

func TestMatchQuery_ProtoBuffer(t *testing.T) {
	// Test MatchQuery ProtoBuffer method
	matchQuery := &MatchQuery{
		FieldName: "title",
		Text:      "test",
	}
	pbQuery, err := matchQuery.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, otsprotocol.QueryType_MATCH_QUERY, *pbQuery.Type)
	assert.NotNil(t, pbQuery.Query)

	// Test MatchQuery ProtoBuffer with all fields
	minShouldMatchValue := int32(1)
	operator := QueryOperator_OR
	weight := float32(2.0)
	matchQueryFull := &MatchQuery{
		FieldName:          "content",
		Text:               "full test",
		MinimumShouldMatch: &minShouldMatchValue,
		Operator:           &operator,
		Weight:             &weight,
		MinShouldMatch:     proto.String("1"),
	}
	pbQueryFull, err := matchQueryFull.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQueryFull)
	assert.Equal(t, otsprotocol.QueryType_MATCH_QUERY, *pbQueryFull.Type)
	assert.NotNil(t, pbQueryFull.Query)
}

func TestMatchQuery_JSONSerialization(t *testing.T) {
	// Test JSON marshaling of MatchQuery
	minShouldMatchValue := int32(2)
	operator := QueryOperator_AND
	weight := float32(1.5)
	matchQuery := &MatchQuery{
		FieldName:          "title",
		Text:               "json test",
		MinimumShouldMatch: &minShouldMatchValue,
		Operator:           &operator,
		Weight:             &weight,
		MinShouldMatch:     proto.String("50%"),
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(matchQuery)
	assert.Nil(t, err)
	assert.NotNil(t, jsonData)

	// Create a new MatchQuery and unmarshal JSON
	newMatchQuery := &MatchQuery{}
	err = json.Unmarshal(jsonData, newMatchQuery)
	assert.Nil(t, err)

	// Verify fields (note: not all fields are preserved in JSON)
	assert.Equal(t, matchQuery.FieldName, newMatchQuery.FieldName)
	assert.Equal(t, matchQuery.Text, newMatchQuery.Text)
}

func TestMatchQuery_EdgeCases(t *testing.T) {
	// Test MatchQuery with empty strings
	emptyMatchQuery := &MatchQuery{
		FieldName: "",
		Text:      "",
	}
	data, err := emptyMatchQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test MatchQuery with special characters
	specialCharMatchQuery := &MatchQuery{
		FieldName: "special@#$%",
		Text:      "text with 中文 and symbols!@#$%^&*()",
	}
	dataSpecial, err := specialCharMatchQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, dataSpecial)

	// Test MatchQuery with very long strings
	longString := string(make([]byte, 10000))
	longMatchQuery := &MatchQuery{
		FieldName: longString,
		Text:      longString,
	}
	dataLong, err := longMatchQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, dataLong)
}
