package search

import (
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
)

type SortOrder int8

const (
	SortOrder_ASC  SortOrder = 0
	SortOrder_DESC SortOrder = 1
)

func (x SortOrder) Enum() *SortOrder {
	p := new(SortOrder)
	*p = x
	return p
}

func (o *SortOrder) ProtoBuffer() (*otsprotocol.SortOrder, error) {
	if o == nil {
		return nil, errors.New("sort order is nil")
	}
	if *o == SortOrder_ASC {
		return otsprotocol.SortOrder_SORT_ORDER_ASC.Enum(), nil
	} else if *o == SortOrder_DESC {
		return otsprotocol.SortOrder_SORT_ORDER_DESC.Enum(), nil
	} else {
		return nil, errors.New("unknown sort order: " + fmt.Sprintf("%#v", *o))
	}
}
