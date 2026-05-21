package search

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestMatchPhraseQuery_Type(t *testing.T) {
	matchPhraseQuery := &MatchPhraseQuery{
		FieldName: "title",
		Text:      "test phrase",
	}
	queryType := matchPhraseQuery.Type()
	assert.Equal(t, QueryType_MatchPhraseQuery, queryType)
}

func TestMatchPhraseQuery_Serialize(t *testing.T) {
	// Test basic MatchPhraseQuery serialization
	matchPhraseQuery := &MatchPhraseQuery{
		FieldName: "title",
		Text:      "search phrase",
	}
	data, err := matchPhraseQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Verify serialized data can be unmarshaled
	pbQuery := &otsprotocol.MatchPhraseQuery{}
	err = proto.Unmarshal(data, pbQuery)
	assert.Nil(t, err)
	assert.Equal(t, "title", *pbQuery.FieldName)
	assert.Equal(t, "search phrase", *pbQuery.Text)
	assert.Nil(t, pbQuery.Weight)
	assert.Nil(t, pbQuery.Slop)
}

func TestMatchPhraseQuery_SerializeWithWeight(t *testing.T) {
	weight := float32(1.5)
	matchPhraseQuery := &MatchPhraseQuery{
		FieldName: "content",
		Text:      "weighted phrase",
		Weight:    &weight,
	}
	data, err := matchPhraseQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Verify serialized data
	pbQuery := &otsprotocol.MatchPhraseQuery{}
	err = proto.Unmarshal(data, pbQuery)
	assert.Nil(t, err)
	assert.Equal(t, "content", *pbQuery.FieldName)
	assert.Equal(t, "weighted phrase", *pbQuery.Text)
	assert.NotNil(t, pbQuery.Weight)
	assert.Equal(t, float32(1.5), *pbQuery.Weight)
	assert.Nil(t, pbQuery.Slop)
}

func TestMatchPhraseQuery_SerializeWithSlop(t *testing.T) {
	slop := int32(2)
	matchPhraseQuery := &MatchPhraseQuery{
		FieldName: "content",
		Text:      "phrase with slop",
		Slop:      &slop,
	}
	data, err := matchPhraseQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Verify serialized data
	pbQuery := &otsprotocol.MatchPhraseQuery{}
	err = proto.Unmarshal(data, pbQuery)
	assert.Nil(t, err)
	assert.Equal(t, "content", *pbQuery.FieldName)
	assert.Equal(t, "phrase with slop", *pbQuery.Text)
	assert.Nil(t, pbQuery.Weight)
	assert.NotNil(t, pbQuery.Slop)
	assert.Equal(t, int32(2), *pbQuery.Slop)
}

func TestMatchPhraseQuery_SerializeWithAllFields(t *testing.T) {
	weight := float32(2.0)
	slop := int32(3)
	matchPhraseQuery := &MatchPhraseQuery{
		FieldName: "description",
		Text:      "full phrase query",
		Weight:    &weight,
		Slop:      &slop,
	}
	data, err := matchPhraseQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Verify serialized data
	pbQuery := &otsprotocol.MatchPhraseQuery{}
	err = proto.Unmarshal(data, pbQuery)
	assert.Nil(t, err)
	assert.Equal(t, "description", *pbQuery.FieldName)
	assert.Equal(t, "full phrase query", *pbQuery.Text)
	assert.NotNil(t, pbQuery.Weight)
	assert.Equal(t, float32(2.0), *pbQuery.Weight)
	assert.NotNil(t, pbQuery.Slop)
	assert.Equal(t, int32(3), *pbQuery.Slop)
}

func TestMatchPhraseQuery_ProtoBuffer(t *testing.T) {
	matchPhraseQuery := &MatchPhraseQuery{
		FieldName: "title",
		Text:      "test phrase",
	}
	pbQuery, err := matchPhraseQuery.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, otsprotocol.QueryType_MATCH_PHRASE_QUERY, *pbQuery.Type)
	assert.NotNil(t, pbQuery.Query)
}

func TestMatchPhraseQuery_ProtoBufferWithAllFields(t *testing.T) {
	weight := float32(1.5)
	slop := int32(2)
	matchPhraseQuery := &MatchPhraseQuery{
		FieldName: "content",
		Text:      "full test phrase",
		Weight:    &weight,
		Slop:      &slop,
	}
	pbQuery, err := matchPhraseQuery.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, otsprotocol.QueryType_MATCH_PHRASE_QUERY, *pbQuery.Type)
	assert.NotNil(t, pbQuery.Query)
}

func TestMatchPhraseQuery_EdgeCases(t *testing.T) {
	// Test MatchPhraseQuery with empty strings
	emptyMatchPhraseQuery := &MatchPhraseQuery{
		FieldName: "",
		Text:      "",
	}
	data, err := emptyMatchPhraseQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test MatchPhraseQuery with special characters
	specialCharMatchPhraseQuery := &MatchPhraseQuery{
		FieldName: "special@#$%",
		Text:      "text with 中文 and symbols!@#$%^&*()",
	}
	dataSpecial, err := specialCharMatchPhraseQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, dataSpecial)

	// Test MatchPhraseQuery with zero weight
	zeroWeight := float32(0.0)
	zeroWeightQuery := &MatchPhraseQuery{
		FieldName: "title",
		Text:      "zero weight",
		Weight:    &zeroWeight,
	}
	dataZeroWeight, err := zeroWeightQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, dataZeroWeight)

	// Test MatchPhraseQuery with zero slop
	zeroSlop := int32(0)
	zeroSlopQuery := &MatchPhraseQuery{
		FieldName: "title",
		Text:      "zero slop",
		Slop:      &zeroSlop,
	}
	dataZeroSlop, err := zeroSlopQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, dataZeroSlop)

	// Test MatchPhraseQuery with negative slop
	negativeSlop := int32(-1)
	negativeSlopQuery := &MatchPhraseQuery{
		FieldName: "title",
		Text:      "negative slop",
		Slop:      &negativeSlop,
	}
	dataNegativeSlop, err := negativeSlopQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, dataNegativeSlop)
}
