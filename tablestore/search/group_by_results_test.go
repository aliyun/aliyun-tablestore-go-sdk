package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
	"testing"
)

func genPBGroupBysResult() *otsprotocol.GroupBysResult {
	pbGroupBysResults := otsprotocol.GroupBysResult{}
	{
		items := []*otsprotocol.GroupByFieldResultItem{
			{
				Key:      proto.String("k1"),
				RowCount: proto.Int64(6),
			},
			{
				Key:      proto.String("k2"),
				RowCount: proto.Int64(9),
			},
		}

		groupByBodyBytes, _ := proto.Marshal(&otsprotocol.GroupByFieldResult{
			GroupByFieldResultItems: items,
		})
		groupByResult := otsprotocol.GroupByResult{
			Name:          proto.String("group_by1"),
			Type:          otsprotocol.GroupByType_GROUP_BY_FIELD.Enum(),
			GroupByResult: groupByBodyBytes,
		}
		pbGroupBysResults.GroupByResults = append(pbGroupBysResults.GroupByResults, &groupByResult)
	}
	{
		items := []*otsprotocol.GroupByFilterResultItem{
			{
				RowCount: proto.Int64(3),
			},
			{
				RowCount: proto.Int64(5),
			},
		}

		groupByBodyBytes, _ := proto.Marshal(&otsprotocol.GroupByFilterResult{
			GroupByFilterResultItems: items,
		})
		groupByResult := otsprotocol.GroupByResult{
			Name:          proto.String("group_by2"),
			Type:          otsprotocol.GroupByType_GROUP_BY_FILTER.Enum(),
			GroupByResult: groupByBodyBytes,
		}
		pbGroupBysResults.GroupByResults = append(pbGroupBysResults.GroupByResults, &groupByResult)
	}
	{
		items := []*otsprotocol.GroupByRangeResultItem{
			{
				From:     proto.Float64(math.Inf(-1)),
				To:       proto.Float64(3),
				RowCount: proto.Int64(333),
			},
			{
				From:     proto.Float64(3),
				To:       proto.Float64(5),
				RowCount: proto.Int64(666),
			},
			{
				From:     proto.Float64(5),
				To:       proto.Float64(math.Inf(1)),
				RowCount: proto.Int64(999),
			},
		}

		groupByBodyBytes, _ := proto.Marshal(&otsprotocol.GroupByRangeResult{
			GroupByRangeResultItems: items,
		})
		groupByResult := otsprotocol.GroupByResult{
			Name:          proto.String("group_by3"),
			Type:          otsprotocol.GroupByType_GROUP_BY_RANGE.Enum(),
			GroupByResult: groupByBodyBytes,
		}
		pbGroupBysResults.GroupByResults = append(pbGroupBysResults.GroupByResults, &groupByResult)
	}
	{
		items := []*otsprotocol.GroupByGeoDistanceResultItem{
			{
				From:     proto.Float64(math.Inf(-1)),
				To:       proto.Float64(3),
				RowCount: proto.Int64(333),
			},
			{
				From:     proto.Float64(3),
				To:       proto.Float64(5),
				RowCount: proto.Int64(666),
			},
			{
				From:     proto.Float64(5),
				To:       proto.Float64(math.Inf(1)),
				RowCount: proto.Int64(999),
			},
		}

		groupByBodyBytes, _ := proto.Marshal(&otsprotocol.GroupByGeoDistanceResult{
			GroupByGeoDistanceResultItems: items,
		})
		groupByResult := otsprotocol.GroupByResult{
			Name:          proto.String("group_by4"),
			Type:          otsprotocol.GroupByType_GROUP_BY_GEO_DISTANCE.Enum(),
			GroupByResult: groupByBodyBytes,
		}
		pbGroupBysResults.GroupByResults = append(pbGroupBysResults.GroupByResults, &groupByResult)
	}
	{
		var value int64 = 1
		var key = rand.Int63()
		items := []*otsprotocol.GroupByHistogramItem{
			{
				Key:   VTInteger(key),
				Value: &value,
			},
		}

		groupByBodyBytes, _ := proto.Marshal(&otsprotocol.GroupByHistogramResult{
			GroupByHistograItems: items,
		})
		groupByResult := otsprotocol.GroupByResult{
			Name:          proto.String("group_by5"),
			Type:          otsprotocol.GroupByType_GROUP_BY_HISTOGRAM.Enum(),
			GroupByResult: groupByBodyBytes,
		}
		pbGroupBysResults.GroupByResults = append(pbGroupBysResults.GroupByResults, &groupByResult)
	}
	{
		var value int64 = 2
		var key int64 = 3
		items := []*otsprotocol.GroupByDateHistogramItem{
			{
				Timestamp: &key,
				RowCount:  &value,
			},
		}

		groupByBodyBytes, _ := proto.Marshal(&otsprotocol.GroupByDateHistogramResult{
			GroupByDateHistogramItems: items,
		})
		groupByResult := otsprotocol.GroupByResult{
			Name:          proto.String("group_by6"),
			Type:          otsprotocol.GroupByType_GROUP_BY_DATE_HISTOGRAM.Enum(),
			GroupByResult: groupByBodyBytes,
		}
		pbGroupBysResults.GroupByResults = append(pbGroupBysResults.GroupByResults, &groupByResult)
	}
	{
		key := []string{"a", "b", "c"}
		rowCount := []int64{10, 20, 30}
		lat := []float64{-45, -90, 90, 45, 90, 45}
		lon := []float64{-180, -135, -180, -135, -135, -90}
		geoGrid := []otsprotocol.GeoGrid{{TopLeft: &otsprotocol.GeoPoint{Lat: &lat[0], Lon: &lon[0]}, BottomRight: &otsprotocol.GeoPoint{Lat: &lat[1], Lon: &lon[1]}},
			{TopLeft: &otsprotocol.GeoPoint{Lat: &lat[2], Lon: &lon[2]}, BottomRight: &otsprotocol.GeoPoint{Lat: &lat[3], Lon: &lon[3]}},
			{TopLeft: &otsprotocol.GeoPoint{Lat: &lat[4], Lon: &lon[4]}, BottomRight: &otsprotocol.GeoPoint{Lat: &lat[5], Lon: &lon[5]}}}
		items := []*otsprotocol.GroupByGeoGridResultItem{
			{
				Key: &key[0],
				RowCount:  &rowCount[0],
				GeoGrid: &geoGrid[0],
			},
			{
				Key: &key[1],
				RowCount:  &rowCount[1],
				GeoGrid: &geoGrid[1],
			},
			{
				Key: &key[2],
				RowCount:  &rowCount[2],
				GeoGrid: &geoGrid[2],
			},
		}

		groupByBodyBytes, _ := proto.Marshal(&otsprotocol.GroupByGeoGridResult{
			GroupByGeoGirdResultItems: items,
		})
		groupByResult := otsprotocol.GroupByResult{
			Name:          proto.String("group_by7"),
			Type:          otsprotocol.GroupByType_GROUP_BY_GEO_GRID.Enum(),
			GroupByResult: groupByBodyBytes,
		}
		pbGroupBysResults.GroupByResults = append(pbGroupBysResults.GroupByResults, &groupByResult)
	}

	return &pbGroupBysResults
}

func TestParseGroupByResultsFromPB(t *testing.T) {
	pbGroupBysResult := genPBGroupBysResult()
	groupByResults, err := ParseGroupByResultsFromPB(pbGroupBysResult.GroupByResults)
	assert.Nil(t, err)
	assert.Equal(t, 7, len(groupByResults.resultMap))
	assert.Equal(t, false, groupByResults.Empty())

	{
		groupByResult, err := groupByResults.GroupByField("group_by1")
		assert.Nil(t, err)
		assert.Equal(t, 2, len(groupByResult.Items))

		assert.Equal(t, "k1", groupByResult.Items[0].Key)
		assert.Equal(t, int64(6), groupByResult.Items[0].RowCount)
		assert.Equal(t, "k2", groupByResult.Items[1].Key)
		assert.Equal(t, int64(9), groupByResult.Items[1].RowCount)
	}
	{
		groupByResult, err := groupByResults.GroupByFilter("group_by2")
		assert.Nil(t, err)
		assert.Equal(t, 2, len(groupByResult.Items))

		assert.Equal(t, int64(3), groupByResult.Items[0].RowCount)
		assert.Equal(t, int64(5), groupByResult.Items[1].RowCount)
	}
	{
		groupByResult, err := groupByResults.GroupByRange("group_by3")
		assert.Nil(t, err)
		assert.Equal(t, 3, len(groupByResult.Items))

		assert.Equal(t, math.Inf(-1), groupByResult.Items[0].From)
		assert.Equal(t, float64(3), groupByResult.Items[0].To)
		assert.Equal(t, int64(333), groupByResult.Items[0].RowCount)

		assert.Equal(t, float64(3), groupByResult.Items[1].From)
		assert.Equal(t, float64(5), groupByResult.Items[1].To)
		assert.Equal(t, int64(666), groupByResult.Items[1].RowCount)

		assert.Equal(t, float64(5), groupByResult.Items[2].From)
		assert.Equal(t, math.Inf(1), groupByResult.Items[2].To)
		assert.Equal(t, int64(999), groupByResult.Items[2].RowCount)
	}
	{
		groupByResult, err := groupByResults.GroupByGeoDistance("group_by4")
		assert.Nil(t, err)
		assert.Equal(t, 3, len(groupByResult.Items))

		assert.Equal(t, math.Inf(-1), groupByResult.Items[0].From)
		assert.Equal(t, float64(3), groupByResult.Items[0].To)
		assert.Equal(t, int64(333), groupByResult.Items[0].RowCount)

		assert.Equal(t, float64(3), groupByResult.Items[1].From)
		assert.Equal(t, float64(5), groupByResult.Items[1].To)
		assert.Equal(t, int64(666), groupByResult.Items[1].RowCount)

		assert.Equal(t, float64(5), groupByResult.Items[2].From)
		assert.Equal(t, math.Inf(1), groupByResult.Items[2].To)
		assert.Equal(t, int64(999), groupByResult.Items[2].RowCount)
	}
	{
		groupByResult, err := groupByResults.GroupByHistogram("group_by5")
		assert.Nil(t, err)
		assert.Equal(t, 1, len(groupByResult.Items))

		assert.Equal(t, int64(1), groupByResult.Items[0].Value)
	}
	{
		groupByResult, err := groupByResults.GroupByDateHistogram("group_by6")
		assert.Nil(t, err)
		assert.Equal(t, 1, len(groupByResult.Items))

		assert.Equal(t, int64(2), groupByResult.Items[0].RowCount)
		assert.Equal(t, int64(3), groupByResult.Items[0].Timestamp)
	}
	{
		groupByResult, err := groupByResults.GroupByGeoGrid("group_by7")
		assert.Nil(t, err)
		assert.Equal(t, 3, len(groupByResult.Items))

		assert.Equal(t, "a", groupByResult.Items[0].Key)
		assert.Equal(t, int64(10), groupByResult.Items[0].RowCount)
		assert.Equal(t, GeoGrid{TopLeft: GeoPoint{Lat: float64(-45), Lon: float64(-180)}, BottomRight: GeoPoint{Lat: float64(-90), Lon: float64(-135)}}, groupByResult.Items[0].GeoGrid)
		assert.Equal(t, "b", groupByResult.Items[1].Key)
		assert.Equal(t, int64(20), groupByResult.Items[1].RowCount)
		assert.Equal(t, GeoGrid{TopLeft: GeoPoint{Lat: float64(90), Lon: float64(-180)}, BottomRight: GeoPoint{Lat: float64(45), Lon: float64(-135)}}, groupByResult.Items[1].GeoGrid)
		assert.Equal(t, "c", groupByResult.Items[2].Key)
		assert.Equal(t, int64(30), groupByResult.Items[2].RowCount)
		assert.Equal(t, GeoGrid{TopLeft: GeoPoint{Lat: float64(90), Lon: float64(-135)}, BottomRight: GeoPoint{Lat: float64(45), Lon: float64(-90)}}, groupByResult.Items[2].GeoGrid)
	}
}
