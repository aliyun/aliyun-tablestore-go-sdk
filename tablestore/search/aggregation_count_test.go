package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCountAggregation_GetName(t *testing.T) {
	agg := CountAggregation{}
	agg.Name("agg1")
	assert.Equal(t, "agg1", agg.AggName)
}

func TestCountAggregation_FieldName(t *testing.T) {
	agg := CountAggregation{}
	agg.FieldName("col1")
	assert.Equal(t, "col1", agg.Field)
}

func TestCountAggregation_GetType(t *testing.T) {
	agg := CountAggregation{}
	assert.Equal(t, agg.GetType(), AggregationCountType)
}

func TestCountAggregation_ProtoBuffer(t *testing.T) {
	agg := CountAggregation{
		AggName: "agg1",
		Field:   "col1",
	}

	pbAgg, err := agg.ProtoBuffer()
	assert.Nil(t, err)

	//type
	assert.Equal(t, pbAgg.GetType(), otsprotocol.AggregationType_AGG_COUNT)

	//name
	assert.Equal(t, pbAgg.GetName(), "agg1")

	//body
	aggBody := new(otsprotocol.CountAggregation)
	proto.Unmarshal(pbAgg.GetBody(), aggBody)

	assert.Equal(t, "col1", *aggBody.FieldName)
}
