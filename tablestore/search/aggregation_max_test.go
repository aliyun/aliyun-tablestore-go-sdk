package search

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
)

func TestMaxAggregation_GetName(t *testing.T) {
	agg := MaxAggregation{}
	agg.Name("agg1")
	assert.Equal(t, "agg1", agg.AggName)
}

func TestMaxAggregation_FieldName(t *testing.T) {
	agg := MaxAggregation{}
	agg.FieldName("col1")
	assert.Equal(t, "col1", agg.Field)
}

func TestMaxAggregation_Missing(t *testing.T) {
	agg := MaxAggregation{}
	agg.Missing(66.66)
	assert.Equal(t, 66.66, agg.MissingValue)
}

func TestMaxAggregation_GetType(t *testing.T) {
	agg := MaxAggregation{}
	assert.Equal(t, agg.GetType(), AggregationMaxType)
}

func TestMaxAggregation_ProtoBuffer(t *testing.T) {
	agg := MaxAggregation{
		AggName:      "agg1",
		Field:        "col1",
		MissingValue: 66.66,
	}

	pbAgg, err := agg.ProtoBuffer()
	assert.Nil(t, err)

	//type
	assert.Equal(t, pbAgg.GetType(), otsprotocol.AggregationType_AGG_MAX)

	//name
	assert.Equal(t, pbAgg.GetName(), "agg1")

	//body
	aggBody := new(otsprotocol.MaxAggregation)
	proto.Unmarshal(pbAgg.GetBody(), aggBody)

	assert.Equal(t, "col1", *aggBody.FieldName)

	missingExpected, err := ToVariantValue(66.66)
	assert.Equal(t, []byte(missingExpected), aggBody.Missing)
}
