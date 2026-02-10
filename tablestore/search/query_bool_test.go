package search

import (
	"encoding/json"
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

// TestBoolQuery_Type tests the Type method of BoolQuery
func TestBoolQuery_Type(t *testing.T) {
	boolQuery := &BoolQuery{}
	assert.Equal(t, QueryType_BoolQuery, boolQuery.Type())
}

// TestBoolQuery_MarshalJSON tests JSON marshaling of BoolQuery
func TestBoolQuery_MarshalJSON(t *testing.T) {
	// Test with all query types
	boolQuery := &BoolQuery{
		MustQueries: []Query{
			&MatchQuery{
				FieldName: "title",
				Text:      "Go OTS",
			},
		},
		ShouldQueries: []Query{
			&MatchQuery{
				FieldName: "content",
				Text:      "tablestore",
			},
		},
		FilterQueries: []Query{
			&RangeQuery{
				FieldName: "age",
				From:      18,
				To:        60,
			},
		},
		MustNotQueries: []Query{
			&TermQuery{
				FieldName: "status",
				Term:      "deleted",
			},
		},
		MinShouldMatch: proto.String("2"),
		Weight:         Float32Ptr(2.0),
	}

	data, err := json.Marshal(boolQuery)
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test with empty queries
	emptyBoolQuery := &BoolQuery{
		MinShouldMatch: proto.String("1"),
		Weight:         Float32Ptr(1.0),
	}
	emptyData, err := json.Marshal(emptyBoolQuery)
	assert.Nil(t, err)
	assert.NotNil(t, emptyData)
}

// TestBoolQuery_UnmarshalJSON tests JSON unmarshaling of BoolQuery
func TestBoolQuery_UnmarshalJSON(t *testing.T) {
	// JSON string with all query types
	jsonStr := `{
		"MustQueries": [
			{
				"Name": "MatchQuery",
				"Query": {
					"FieldName": "title",
					"Text": "Go OTS"
				}
			}
		],
		"ShouldQueries": [
			{
				"Name": "MatchQuery",
				"Query": {
					"FieldName": "content",
					"Text": "tablestore"
				}
			}
		],
		"FilterQueries": [
			{
				"Name": "RangeQuery",
				"Query": {
					"FieldName": "age",
					"From": 18,
					"To": 60
				}
			}
		],
		"MustNotQueries": [
			{
				"Name": "TermQuery",
				"Query": {
					"FieldName": "status",
					"Term": "deleted"
				}
			}
		],
		"MinShouldMatch": "2",
		"Weight": 2.0
	}`

	boolQuery := &BoolQuery{}
	err := json.Unmarshal([]byte(jsonStr), boolQuery)
	assert.Nil(t, err)

	// Verify all fields
	assert.Equal(t, proto.String("2"), boolQuery.MinShouldMatch)
	assert.Equal(t, float32(2.0), *boolQuery.Weight)

	// Verify MustQueries
	assert.Equal(t, 1, len(boolQuery.MustQueries))
	matchQuery1, ok := boolQuery.MustQueries[0].(*MatchQuery)
	assert.True(t, ok)
	assert.Equal(t, "title", matchQuery1.FieldName)
	assert.Equal(t, "Go OTS", matchQuery1.Text)

	// Verify ShouldQueries
	assert.Equal(t, 1, len(boolQuery.ShouldQueries))
	matchQuery2, ok := boolQuery.ShouldQueries[0].(*MatchQuery)
	assert.True(t, ok)
	assert.Equal(t, "content", matchQuery2.FieldName)
	assert.Equal(t, "tablestore", matchQuery2.Text)

	// Verify FilterQueries
	assert.Equal(t, 1, len(boolQuery.FilterQueries))
	rangeQuery, ok := boolQuery.FilterQueries[0].(*RangeQuery)
	assert.True(t, ok)
	assert.Equal(t, "age", rangeQuery.FieldName)
	assert.InDelta(t, 18, rangeQuery.From, 1e-9)
	assert.InDelta(t, 60, rangeQuery.To, 1e-9)

	// Verify MustNotQueries
	assert.Equal(t, 1, len(boolQuery.MustNotQueries))
	termQuery, ok := boolQuery.MustNotQueries[0].(*TermQuery)
	assert.True(t, ok)
	assert.Equal(t, "status", termQuery.FieldName)
	assert.Equal(t, "deleted", termQuery.Term)

	// Test with empty queries
	emptyJson := `{
		"MustQueries": [],
		"ShouldQueries": [],
		"FilterQueries": [],
		"MustNotQueries": [],
		"MinShouldMatch": "1",
		"Weight": 1.0
	}`
	emptyBoolQuery := &BoolQuery{}
	err = json.Unmarshal([]byte(emptyJson), emptyBoolQuery)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(emptyBoolQuery.MustQueries))
	assert.Equal(t, 0, len(emptyBoolQuery.ShouldQueries))
	assert.Equal(t, 0, len(emptyBoolQuery.FilterQueries))
	assert.Equal(t, 0, len(emptyBoolQuery.MustNotQueries))
}

// TestBoolQuery_Serialize tests serialization of BoolQuery
func TestBoolQuery_Serialize(t *testing.T) {
	// Normal case
	boolQuery := &BoolQuery{
		MustQueries: []Query{
			&MatchQuery{
				FieldName: "title",
				Text:      "Go OTS",
			},
		},
		ShouldQueries: []Query{
			&MatchQuery{
				FieldName: "content",
				Text:      "tablestore",
			},
		},
		FilterQueries: []Query{
			&RangeQuery{
				FieldName: "age",
				From:      18,
				To:        60,
			},
		},
		MustNotQueries: []Query{
			&TermQuery{
				FieldName: "status",
				Term:      "deleted",
			},
		},
		MinShouldMatch: proto.String("2"),
		Weight:         Float32Ptr(2.0),
	}

	data, err := boolQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test with empty queries
	emptyBoolQuery := &BoolQuery{
		MinShouldMatch: proto.String("1"),
		Weight:         Float32Ptr(1.0),
	}
	data, err = emptyBoolQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test error case with invalid query
	invalidBoolQuery := &BoolQuery{
		MustQueries: []Query{
			&MockInvalidQuery{},
		},
	}
	data, err = invalidBoolQuery.Serialize()
	assert.NotNil(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "protobuf error")
}

// TestBoolQuery_ProtoBuffer tests ProtoBuffer conversion of BoolQuery
func TestBoolQuery_ProtoBuffer(t *testing.T) {
	// Normal case
	boolQuery := &BoolQuery{
		MustQueries: []Query{
			&MatchQuery{
				FieldName: "title",
				Text:      "Go OTS",
			},
		},
		ShouldQueries: []Query{
			&MatchQuery{
				FieldName: "content",
				Text:      "tablestore",
			},
		},
		FilterQueries: []Query{
			&RangeQuery{
				FieldName: "age",
				From:      18,
				To:        60,
			},
		},
		MustNotQueries: []Query{
			&TermQuery{
				FieldName: "status",
				Term:      "deleted",
			},
		},
		MinShouldMatch: proto.String("2"),
		Weight:         Float32Ptr(2.0),
	}

	pbQuery, err := boolQuery.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, otsprotocol.QueryType_BOOL_QUERY, *pbQuery.Type)

	// Test with empty queries
	emptyBoolQuery := &BoolQuery{
		MinShouldMatch: proto.String("1"),
		Weight:         Float32Ptr(1.0),
	}
	pbQuery, err = emptyBoolQuery.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, otsprotocol.QueryType_BOOL_QUERY, *pbQuery.Type)
}

// TestBoolQuery_EdgeCases tests edge cases for BoolQuery
func TestBoolQuery_EdgeCases(t *testing.T) {
	// Test with nil query slices
	nilBoolQuery := &BoolQuery{
		MustQueries:    nil,
		ShouldQueries:  nil,
		FilterQueries:  nil,
		MustNotQueries: nil,
		MinShouldMatch: proto.String("1"),
		Weight:         Float32Ptr(1.0),
	}

	data, err := nilBoolQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	pbQuery, err := nilBoolQuery.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, otsprotocol.QueryType_BOOL_QUERY, *pbQuery.Type)

	// Test JSON marshal/unmarshal round trip
	originalQuery := &BoolQuery{
		MustQueries: []Query{
			&MatchQuery{
				FieldName: "title",
				Text:      "test",
			},
		},
		MinShouldMatch: proto.String("1"),
		Weight:         Float32Ptr(1.5),
	}

	jsonData, err := json.Marshal(originalQuery)
	assert.Nil(t, err)

	newQuery := &BoolQuery{}
	err = json.Unmarshal(jsonData, newQuery)
	assert.Nil(t, err)

	// Note: Due to JSON serialization approach, not all fields are preserved exactly
	assert.Equal(t, originalQuery.MinShouldMatch, newQuery.MinShouldMatch)
	assert.Equal(t, *originalQuery.Weight, *newQuery.Weight)
}

// TestBoolQuery_MinShouldMatchPercentage tests BoolQuery with percentage MinShouldMatch
func TestBoolQuery_MinShouldMatchPercentage(t *testing.T) {
	// Test with percentage MinShouldMatch during JSON marshal/unmarshal
	percentageBoolQuery := &BoolQuery{
		MustQueries: []Query{
			&MatchQuery{
				FieldName: "title",
				Text:      "test",
			},
		},
		ShouldQueries: []Query{
			&MatchQuery{
				FieldName: "content",
				Text:      "example",
			},
			&MatchQuery{
				FieldName: "description",
				Text:      "sample",
			},
		},
		MinShouldMatch: proto.String("50%"), // 50% percentage format
		Weight:         Float32Ptr(1.0),
	}

	// Test JSON marshaling with percentage
	data, err := json.Marshal(percentageBoolQuery)
	assert.Nil(t, err)
	assert.NotNil(t, data)

	// Test JSON unmarshaling with percentage
	unmarshaledQuery := &BoolQuery{}
	err = json.Unmarshal(data, unmarshaledQuery)
	assert.Nil(t, err)
	assert.Equal(t, proto.String("50%"), unmarshaledQuery.MinShouldMatch)
	assert.Equal(t, float32(1.0), *unmarshaledQuery.Weight)
	assert.Equal(t, 1, len(unmarshaledQuery.MustQueries))
	assert.Equal(t, 2, len(unmarshaledQuery.ShouldQueries))

	// Test serialization with percentage
	serializedData, err := percentageBoolQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, serializedData)

	// Test ProtoBuffer conversion with percentage
	pbQuery, err := percentageBoolQuery.ProtoBuffer()
	assert.Nil(t, err)
	assert.NotNil(t, pbQuery)
	assert.Equal(t, otsprotocol.QueryType_BOOL_QUERY, *pbQuery.Type)
}
