package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
)

const (
	FirstWhenMissing = "_first"
	LastWhenMissing  = "_last"
)

type NestedFilter struct {
	Path   string
	Filter Query
}

func (f *NestedFilter) ProtoBuffer() (*otsprotocol.NestedFilter, error) {
	pbF := &otsprotocol.NestedFilter{
		Path: &f.Path,
	}
	pbQ, err := f.Filter.ProtoBuffer()
	if err != nil {
		return nil, err
	}
	pbF.Filter = pbQ
	return pbF, err
}

type FieldSort struct {
	FieldName    string
	Order        *SortOrder
	Mode         *SortMode
	NestedFilter *NestedFilter
	MissingValue interface{} // When some rows of the sorting field have no fill values, the sorting behavior supports three methods: 1. Set to FirstWhenMissing, which places rows with missing sort field values at the front; 2. Set to LastWhenMissing, which places rows with missing sort field values at the back; 3. Customize a value, which uses a specified value for sorting when the sort field value is missing.
	// Deprecated: use `MissingFields` instead
	MissingField *string
	MissingFields []string
}

func NewFieldSort(fieldName string, order SortOrder) *FieldSort {
	return &FieldSort{
		FieldName: fieldName,
		Order:     order.Enum(),
	}
}

func (s *FieldSort) ProtoBuffer() (*otsprotocol.Sorter, error) {
	pbFieldSort := &otsprotocol.FieldSort{
		FieldName: &s.FieldName,
	}
	if s.Order != nil {
		pbOrder, err := s.Order.ProtoBuffer()
		if err != nil {
			return nil, err
		}
		pbFieldSort.Order = pbOrder
	}
	if s.Mode != nil {
		pbMode, err := s.Mode.ProtoBuffer()
		if err != nil {
			return nil, err
		}
		if pbMode != nil {
			pbFieldSort.Mode = pbMode
		}
	}
	if s.NestedFilter != nil {
		pbFilter, err := s.NestedFilter.ProtoBuffer()
		if err != nil {
			return nil, err
		}
		pbFieldSort.NestedFilter = pbFilter
	}
	if s.MissingField != nil {
		pbFieldSort.MissingField = s.MissingField
	}
	if len(s.MissingFields) != 0 {
		pbFieldSort.MissingFields = s.MissingFields
	}
	//missingValue
	if s.MissingValue != nil {
		vt, err := ToVariantValue(s.MissingValue)
		if err != nil {
			return nil, err
		}
		pbFieldSort.MissingValue = []byte(vt)
	}
	pbSorter := &otsprotocol.Sorter{
		FieldSort: pbFieldSort,
	}
	return pbSorter, nil
}
