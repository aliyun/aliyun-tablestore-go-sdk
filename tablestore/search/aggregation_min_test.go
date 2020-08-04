package search

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
)

func TestMinAggregation_GetName(t *testing.T) {
	agg := MinAggregation{}
	agg.Name("agg1")
	assert.Equal(t, "agg1", agg.AggName)
}

func TestMinAggregation_FieldName(t *testing.T) {
	agg := MinAggregation{}
	agg.FieldName("col1")
	assert.Equal(t, "col1", agg.Field)
}

func TestMinAggregation_Missing(t *testing.T) {
	agg := MinAggregation{}
	agg.Missing(66.66)
	assert.Equal(t, 66.66, agg.MissingValue)
}

func TestMinAggregation_GetType(t *testing.T) {
	agg := MinAggregation{}
	assert.Equal(t, agg.GetType(), AggregationMinType)
}

func TestMinAggregation_ProtoBuffer(t *testing.T) {
	agg := MinAggregation{
		AggName:      "agg1",
		Field:        "col1",
		MissingValue: 66.66,
	}

	pbAgg, err := agg.ProtoBuffer()
	assert.Nil(t, err)

	//type
	assert.Equal(t, pbAgg.GetType(), otsprotocol.AggregationType_AGG_MIN)

	//name
	assert.Equal(t, pbAgg.GetName(), "agg1")

	//body
	aggBody := new(otsprotocol.MinAggregation)
	proto.Unmarshal(pbAgg.GetBody(), aggBody)

	assert.Equal(t, "col1", *aggBody.FieldName)

	missingExpected, err := ToVariantValue(66.66)
	assert.Equal(t, []byte(missingExpected), aggBody.Missing)
}
