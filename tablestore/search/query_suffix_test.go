package search

import (
	"encoding/json"
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestSuffixQuery_Type(t *testing.T) {
	query := &SuffixQuery{}
	assert.Equal(t, QueryType_SuffixQuery, query.Type())
}

func TestSuffixQuery_QueryTypeRoundTrip(t *testing.T) {
	assert.Equal(t, "SuffixQuery", QueryType_SuffixQuery.String())

	assert.Equal(t, QueryType_SuffixQuery, ToQueryType("SuffixQuery"))

	data := json.RawMessage(`{"FieldName":"Col_Fuzzy_Keyword","Suffix":"zhou"}`)
	q, err := UnmarshalQuery("SuffixQuery", data)
	assert.NoError(t, err)
	suffixQuery, ok := q.(*SuffixQuery)
	assert.True(t, ok)
	assert.Equal(t, "Col_Fuzzy_Keyword", suffixQuery.FieldName)
	assert.Equal(t, "zhou", suffixQuery.Suffix)
}

func TestSuffixQuery_Serialize(t *testing.T) {
	query := &SuffixQuery{
		FieldName: "Col_Fuzzy_Keyword",
		Suffix:    "hai",
		Weight:    proto.Float32(2.0),
	}

	data, err := query.Serialize()
	assert.NoError(t, err)
	assert.NotNil(t, data)

	pb := &otsprotocol.SuffixQuery{}
	err = proto.Unmarshal(data, pb)
	assert.NoError(t, err)
	assert.Equal(t, "Col_Fuzzy_Keyword", pb.GetFieldName())
	assert.Equal(t, "hai", pb.GetSuffix())
	assert.InDelta(t, 2.0, pb.GetWeight(), 0.001)
}

func TestSuffixQuery_Serialize_NilWeight(t *testing.T) {
	query := &SuffixQuery{
		FieldName: "Col_Fuzzy_Keyword",
		Suffix:    "test",
	}

	data, err := query.Serialize()
	assert.NoError(t, err)

	pb := &otsprotocol.SuffixQuery{}
	err = proto.Unmarshal(data, pb)
	assert.NoError(t, err)
	assert.NotNil(t, pb.Weight)
	assert.InDelta(t, 1.0, pb.GetWeight(), 0.001)
}

func TestSuffixQuery_ProtoBuffer(t *testing.T) {
	query := &SuffixQuery{
		FieldName: "Col_Fuzzy_Keyword",
		Suffix:    "hangzhou",
		Weight:    proto.Float32(1.5),
	}

	pbQuery, err := query.ProtoBuffer()
	assert.NoError(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, otsprotocol.QueryType_SUFFIX_QUERY, pbQuery.GetType())
}

func TestSuffixQuery_JSON_Marshal(t *testing.T) {
	query := &SuffixQuery{
		FieldName: "Col_Fuzzy_Keyword",
		Suffix:    "test",
		Weight:    proto.Float32(2.0),
	}

	data, err := json.Marshal(query)
	assert.NoError(t, err)
	assert.Contains(t, string(data), `"FieldName":"Col_Fuzzy_Keyword"`)
	assert.Contains(t, string(data), `"Suffix":"test"`)
	assert.Contains(t, string(data), `"Weight":2`)
}

func TestSuffixQuery_JSON_Unmarshal(t *testing.T) {
	jsonData := `{"FieldName":"Col_Fuzzy_Keyword","Suffix":"hai","Weight":1.5}`

	query := &SuffixQuery{}
	err := json.Unmarshal([]byte(jsonData), query)
	assert.NoError(t, err)
	assert.Equal(t, "Col_Fuzzy_Keyword", query.FieldName)
	assert.Equal(t, "hai", query.Suffix)
	assert.InDelta(t, 1.5, *query.Weight, 0.001)
}

func TestSuffixQuery_EmptyFieldName(t *testing.T) {
	query := &SuffixQuery{
		FieldName: "",
		Suffix:    "test",
	}

	data, err := query.Serialize()
	assert.NoError(t, err)
	assert.NotNil(t, data)
}

func TestSuffixQuery_EmptySuffix(t *testing.T) {
	query := &SuffixQuery{
		FieldName: "Col_Fuzzy_Keyword",
		Suffix:    "",
	}

	data, err := query.Serialize()
	assert.NoError(t, err)
	assert.NotNil(t, data)
}
