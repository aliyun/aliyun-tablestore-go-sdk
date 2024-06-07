package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGroupByComposite_Serialize(t *testing.T) {
	groupByComposite := &GroupByComposite{
		Size: proto.Int32(2000),
		SuggestedSize: proto.Int32(2000),
		GroupByName: "groupByComposite",
		SourceGroupByList: []GroupBy{
			&GroupByField{
				AggName: "groupByField",
				Field:   "Keyowrd",
			},
		},
		SubAggList: []Aggregation{
			&MaxAggregation{
				AggName: "MaxAgg",
				Field:   "Long",
			},
		},
		SubGroupByList: []GroupBy{
			&GroupByField{
				AggName: "groupByField",
				Field:   "Keyword1",
			},
		},
	}

	bytes, err := groupByComposite.Serialize()
	assert.Nil(t, err)

	pbGroupByComposite := new(otsprotocol.GroupByComposite)
	err = proto.Unmarshal(bytes, pbGroupByComposite)
	assert.Nil(t, err)

	assert.NotNil(t, pbGroupByComposite.Size)
	assert.Equal(t, int32(2000), *pbGroupByComposite.Size)
	assert.NotNil(t, pbGroupByComposite.SuggestedSize)
	assert.Equal(t, int32(2000), *pbGroupByComposite.SuggestedSize)
	assert.NotNil(t, pbGroupByComposite.Sources)
	assert.Equal(t, len(groupByComposite.SourceGroupByList), len(pbGroupByComposite.Sources.GroupBys))
	assert.Equal(t, groupByComposite.SourceGroupByList[0].GetName(), pbGroupByComposite.Sources.GroupBys[0].GetName())
	assert.Equal(t, int64(groupByComposite.SourceGroupByList[0].GetType()), int64(*pbGroupByComposite.Sources.GroupBys[0].Type))
	sourceGroupByBytes, _ := groupByComposite.SourceGroupByList[0].Serialize()
	assert.Equal(t, sourceGroupByBytes, pbGroupByComposite.GetSources().GroupBys[0].GetBody())

	assert.NotNil(t, pbGroupByComposite.SubAggs)
	assert.Equal(t, len(groupByComposite.SubAggList), len(pbGroupByComposite.SubAggs.Aggs))
	assert.Equal(t, groupByComposite.SubAggList[0].GetName(), pbGroupByComposite.GetSubAggs().Aggs[0].GetName())
	subAggBytes, _ := groupByComposite.SubAggList[0].Serialize()
	assert.Equal(t, subAggBytes, pbGroupByComposite.GetSubAggs().Aggs[0].GetBody())

	assert.NotNil(t, pbGroupByComposite.SubGroupBys)
	assert.Equal(t, len(groupByComposite.SubGroupByList), len(pbGroupByComposite.SubGroupBys.GroupBys))
	assert.Equal(t, groupByComposite.SubGroupByList[0].GetName(), pbGroupByComposite.GetSubGroupBys().GroupBys[0].GetName())
	subGroupByBytes, _ := groupByComposite.SubGroupByList[0].Serialize()
	assert.Equal(t, subGroupByBytes, pbGroupByComposite.GetSubGroupBys().GroupBys[0].GetBody())
}
