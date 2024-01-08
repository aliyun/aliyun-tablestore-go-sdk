package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDistinctCountAggregation_GetName(t *testing.T) {
	agg := DistinctCountAggregation{}
	agg.Name("agg1")
	assert.Equal(t, "agg1", agg.AggName)
}

func TestDistinctCountAggregation_FieldName(t *testing.T) {
	agg := DistinctCountAggregation{}
	agg.FieldName("col1")
	assert.Equal(t, "col1", agg.Field)
}

func TestDistinctAggregation_Missing(t *testing.T) {
	agg := DistinctCountAggregation{}
	agg.Missing(66.66)
	assert.Equal(t, 66.66, agg.MissingValue)
}

func TestDistinctCountAggregation_GetType(t *testing.T) {
	agg := DistinctCountAggregation{}
	assert.Equal(t, agg.GetType(), AggregationDistinctCountType)
}

func TestDistinctCountAggregation_ProtoBuffer(t *testing.T) {
	agg := DistinctCountAggregation{
		AggName: "agg1",
		Field:   "col1",
	}

	pbAgg, err := agg.ProtoBuffer()
	assert.Nil(t, err)

	//type
	assert.Equal(t, pbAgg.GetType(), otsprotocol.AggregationType_AGG_DISTINCT_COUNT)

	//name
	assert.Equal(t, pbAgg.GetName(), "agg1")

	//body
	aggBody := new(otsprotocol.DistinctCountAggregation)
	proto.Unmarshal(pbAgg.GetBody(), aggBody)

	assert.Equal(t, "col1", *aggBody.FieldName)
}
