package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTopRowsAggregation_GetName(t *testing.T) {
	agg := TopRowsAggregation{}
	agg.SetName("agg1")
	assert.Equal(t, "agg1", agg.AggName)
}

func TestTopRowsAggregation_GetType(t *testing.T) {
	agg := TopRowsAggregation{}
	assert.Equal(t, AggregationTopRowsType, agg.GetType())
}

func TestTopRowsAggregation_Limit(t *testing.T) {
	agg := TopRowsAggregation{}
	agg.SetLimit(1)
	assert.Equal(t, int32(1), *agg.Limit)
}

func TestTopRowsAggregation_ProtoBuffer(t *testing.T) {

	var limit int32 = 1
	agg := TopRowsAggregation{
		AggName: "agg1",
		Limit:   &limit,
		Sort:    nil,
	}

	pbAgg, err := agg.ProtoBuffer()
	assert.Nil(t, err)

	assert.Equal(t, pbAgg.GetType(), otsprotocol.AggregationType_AGG_TOP_ROWS)

	assert.Equal(t, pbAgg.GetName(), "agg1")

	aggBody := new(otsprotocol.TopRowsAggregation)
	proto.Unmarshal(pbAgg.GetBody(), aggBody)

	assert.Equal(t, int32(1), aggBody.GetLimit())
}
