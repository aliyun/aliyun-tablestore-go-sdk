package search

import (
	"github.com/golang/protobuf/proto"
	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAvgAggregation_GetName(t *testing.T) {
	agg := AvgAggregation{}
	agg.Name("agg1")
	assert.Equal(t, "agg1", agg.AggName)
}

func TestAvgAggregation_FieldName(t *testing.T) {
	agg := AvgAggregation{}
	agg.FieldName("col1")
	assert.Equal(t, "col1", agg.Field)
}

func TestAvgAggregation_Missing(t *testing.T) {
	agg := AvgAggregation{}
	agg.Missing(66.66)
	assert.Equal(t, 66.66, agg.MissingValue)
}

func TestAvgAggregation_GetType(t *testing.T) {
	agg := AvgAggregation{}
	assert.Equal(t, agg.GetType(), AggregationAvgType)
}

func TestAvgAggregation_ProtoBuffer(t *testing.T) {
	agg := AvgAggregation{
		AggName:      "agg1",
		Field:        "col1",
		MissingValue: 66.66,
	}

	pbAgg, err := agg.ProtoBuffer()
	assert.Nil(t, err)

	//type
	assert.Equal(t, pbAgg.GetType(), otsprotocol.AggregationType_AGG_AVG)

	//name
	assert.Equal(t, pbAgg.GetName(), "agg1")

	//body
	aggBody := new(otsprotocol.AvgAggregation)
	proto.Unmarshal(pbAgg.GetBody(), aggBody)

	assert.Equal(t, "col1", *aggBody.FieldName)

	missingExpected, err := ToVariantValue(66.66)
	assert.Equal(t, []byte(missingExpected), aggBody.Missing)
}
