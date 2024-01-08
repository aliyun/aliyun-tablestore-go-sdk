package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDocSort_ProtoBuffer(t *testing.T) {
	docSort := &DocSort{
		SortOrder: SortOrder_DESC.Enum(),
	}
	
	pbDocSort, err := docSort.ProtoBuffer()
	assert.Nil(t, err)
	assert.Nil(t, pbDocSort.PkSort)
	assert.Nil(t, pbDocSort.GeoDistanceSort)
	assert.Nil(t, pbDocSort.FieldSort)
	assert.Nil(t, pbDocSort.ScoreSort)
	assert.Equal(t, pbDocSort.DocSort.Order.String(), otsprotocol.SortOrder_SORT_ORDER_DESC.String())
}
