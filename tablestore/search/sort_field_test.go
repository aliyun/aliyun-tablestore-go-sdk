package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFieldSort(t *testing.T) {

	fieldSort := FieldSort{}
	fieldSort.MissingField = proto.String("abc")
	fieldSort.FieldName = "fieldName"
	fieldSort.MissingValue = 20
	fieldSort.Order = SortOrder_ASC.Enum()
	fieldSort.Mode = SortMode_Max.Enum()
	fieldSort.NestedFilter = &NestedFilter{
		Path: "path",
	}

	assert.Equal(t, "fieldName", fieldSort.FieldName)
	assert.Equal(t, "abc", *fieldSort.MissingField)
	assert.Equal(t, 20, fieldSort.MissingValue)
	assert.Equal(t, SortOrder_ASC.Enum(), fieldSort.Order)
	assert.Equal(t, SortMode_Max.Enum(), fieldSort.Mode)
	assert.Equal(t, "path", fieldSort.NestedFilter.Path)

}

func TestFieldSort_ProtoBuffer(t *testing.T) {

	fieldSort := FieldSort{}
	fieldSort.MissingField = proto.String("abc")
	fieldSort.FieldName = "fieldName"
	fieldSort.MissingValue = 20
	fieldSort.Order = SortOrder_ASC.Enum()
	fieldSort.Mode = SortMode_Max.Enum()
	fieldSort.NestedFilter = &NestedFilter{
		Path:   "path",
		Filter: &MatchAllQuery{},
	}

	pbFieldSort, err := fieldSort.ProtoBuffer()
	assert.Nil(t, err)
	assert.Equal(t, "fieldName", pbFieldSort.FieldSort.GetFieldName())
	assert.Equal(t, "abc", pbFieldSort.FieldSort.GetMissingField())
	missingExpected, err := ToVariantValue(20)
	assert.Equal(t, []byte(missingExpected), pbFieldSort.FieldSort.GetMissingValue())
	assert.Equal(t, otsprotocol.SortOrder_SORT_ORDER_ASC, pbFieldSort.FieldSort.GetOrder())
	assert.Equal(t, otsprotocol.SortMode_SORT_MODE_MAX, pbFieldSort.FieldSort.GetMode())
	assert.Equal(t, "path", pbFieldSort.FieldSort.GetNestedFilter().GetPath())
}
