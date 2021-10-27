package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPercentilesAggregation_GetName(t *testing.T) {
	agg := PercentilesAggregation{}
	agg.SetName("agg1")
	assert.Equal(t, "agg1", agg.AggName)
}

func TestPercentilesAggregation_FieldName(t *testing.T) {
	agg := PercentilesAggregation{}
	agg.SetFieldName("col1")
	assert.Equal(t, "col1", agg.Field)
}

func TestPercentilesAggregation_Missing(t *testing.T) {
	agg := PercentilesAggregation{}
	agg.SetMissing(66.66)
	assert.Equal(t, 66.66, agg.MissingValue)
}

func TestPercentAggregation_GetType(t *testing.T) {
	agg := PercentilesAggregation{}
	assert.Equal(t, AggregationPercentilesType, agg.GetType())
}

func TestPercentAggregation_GetPercents(t *testing.T) {
	agg := PercentilesAggregation{}
	var percents = make([]float64, 0)
	percents = append(percents, 1.1)
	agg.SetPercents(percents)
	assert.Equal(t, 1.1, agg.GetPercents()[0])
}

func TestPercentAggregation_ProtoBuffer(t *testing.T) {
	agg := PercentilesAggregation{
		AggName:      "agg1",
		Field:        "col1",
		MissingValue: 66.66,
	}

	pbAgg, err := agg.ProtoBuffer()
	assert.Nil(t, err)

	//type
	assert.Equal(t, pbAgg.GetType(), otsprotocol.AggregationType_AGG_PERCENTILES)

	//name
	assert.Equal(t, pbAgg.GetName(), "agg1")

	//body
	aggBody := new(otsprotocol.PercentilesAggregation)
	proto.Unmarshal(pbAgg.GetBody(), aggBody)

	assert.Equal(t, "col1", *aggBody.FieldName)

	missingExpected, err := ToVariantValue(66.66)
	assert.Equal(t, []byte(missingExpected), aggBody.Missing)
}
