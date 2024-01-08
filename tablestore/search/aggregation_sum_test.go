package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSumAggregation_GetName(t *testing.T) {
	agg := SumAggregation{}
	agg.Name("agg1")
	assert.Equal(t, "agg1", agg.AggName)
}

func TestSumAggregation_FieldName(t *testing.T) {
	agg := SumAggregation{}
	agg.FieldName("col1")
	assert.Equal(t, "col1", agg.Field)
}

func TestSumAggregation_Missing(t *testing.T) {
	agg := SumAggregation{}
	agg.Missing(66.66)
	assert.Equal(t, 66.66, agg.MissingValue)
}

func TestSumAggregation_GetType(t *testing.T) {
	agg := SumAggregation{}
	assert.Equal(t, agg.GetType(), AggregationSumType)
}

func TestSumAggregation_ProtoBuffer(t *testing.T) {
	agg := SumAggregation{
		AggName:      "agg1",
		Field:        "col1",
		MissingValue: 66.66,
	}

	pbAgg, err := agg.ProtoBuffer()
	assert.Nil(t, err)

	//type
	assert.Equal(t, pbAgg.GetType(), otsprotocol.AggregationType_AGG_SUM)

	//name
	assert.Equal(t, pbAgg.GetName(), "agg1")

	//body
	aggBody := new(otsprotocol.SumAggregation)
	proto.Unmarshal(pbAgg.GetBody(), aggBody)

	assert.Equal(t, "col1", *aggBody.FieldName)

	missingExpected, err := ToVariantValue(66.66)
	assert.Equal(t, []byte(missingExpected), aggBody.Missing)
}
