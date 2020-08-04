package search

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
)

func genPBAggregationsResult() *otsprotocol.AggregationsResult {
	pbAggsResult := otsprotocol.AggregationsResult{}
	{
		aggBodyBytes, _ := proto.Marshal(&otsprotocol.AvgAggregationResult{
			Value: proto.Float64(6.6),
		})

		aggResult := otsprotocol.AggregationResult{
			Name:      proto.String("agg1"),
			Type:      otsprotocol.AggregationType_AGG_AVG.Enum(),
			AggResult: aggBodyBytes,
		}
		pbAggsResult.AggResults = append(pbAggsResult.AggResults, &aggResult)
	}
	{
		aggBodyBytes, _ := proto.Marshal(&otsprotocol.DistinctCountAggregationResult{
			Value: proto.Int64(6),
		})

		aggResult := otsprotocol.AggregationResult{
			Name:      proto.String("agg2"),
			Type:      otsprotocol.AggregationType_AGG_DISTINCT_COUNT.Enum(),
			AggResult: aggBodyBytes,
		}
		pbAggsResult.AggResults = append(pbAggsResult.AggResults, &aggResult)
	}
	{
		aggBodyBytes, _ := proto.Marshal(&otsprotocol.MaxAggregationResult{
			Value: proto.Float64(6.6),
		})

		aggResult := otsprotocol.AggregationResult{
			Name:      proto.String("agg3"),
			Type:      otsprotocol.AggregationType_AGG_MAX.Enum(),
			AggResult: aggBodyBytes,
		}
		pbAggsResult.AggResults = append(pbAggsResult.AggResults, &aggResult)
	}
	{
		aggBodyBytes, _ := proto.Marshal(&otsprotocol.MinAggregationResult{
			Value: proto.Float64(6.6),
		})

		aggResult := otsprotocol.AggregationResult{
			Name:      proto.String("agg4"),
			Type:      otsprotocol.AggregationType_AGG_MIN.Enum(),
			AggResult: aggBodyBytes,
		}
		pbAggsResult.AggResults = append(pbAggsResult.AggResults, &aggResult)
	}
	{
		aggBodyBytes, _ := proto.Marshal(&otsprotocol.SumAggregationResult{
			Value: proto.Float64(6.6),
		})

		aggResult := otsprotocol.AggregationResult{
			Name:      proto.String("agg5"),
			Type:      otsprotocol.AggregationType_AGG_SUM.Enum(),
			AggResult: aggBodyBytes,
		}
		pbAggsResult.AggResults = append(pbAggsResult.AggResults, &aggResult)
	}
	{
		aggBodyBytes, _ := proto.Marshal(&otsprotocol.CountAggregationResult{
			Value: proto.Int64(6),
		})

		aggResult := otsprotocol.AggregationResult{
			Name:      proto.String("agg6"),
			Type:      otsprotocol.AggregationType_AGG_COUNT.Enum(),
			AggResult: aggBodyBytes,
		}
		pbAggsResult.AggResults = append(pbAggsResult.AggResults, &aggResult)
	}
	return &pbAggsResult
}

func TestParseAggregationResultsFromPB(t *testing.T) {
	pbAggsResult := genPBAggregationsResult()
	aggResults, _ := ParseAggregationResultsFromPB(pbAggsResult.AggResults)
	assert.Equal(t, 6, len(aggResults.resultMap))
	assert.Equal(t, false, aggResults.Empty())

	{
		aggResult, err := aggResults.Avg("agg1")
		assert.Nil(t, err)
		assert.Equal(t, 6.6, aggResult.Value)
	}
	{
		aggResult, err := aggResults.DistinctCount("agg2")
		assert.Nil(t, err)
		assert.Equal(t, int64(6), aggResult.Value)
	}
	{
		aggResult, err := aggResults.Max("agg3")
		assert.Nil(t, err)
		assert.Equal(t, float64(6.6), aggResult.Value)
	}
	{
		aggResult, err := aggResults.Min("agg4")
		assert.Nil(t, err)
		assert.Equal(t, float64(6.6), aggResult.Value)
	}
	{
		aggResult, err := aggResults.Sum("agg5")
		assert.Nil(t, err)
		assert.Equal(t, float64(6.6), aggResult.Value)
	}
	{
		aggResult, err := aggResults.Count("agg6")
		assert.Nil(t, err)
		assert.Equal(t, int64(6), aggResult.Value)
	}
}
