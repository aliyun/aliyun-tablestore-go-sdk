package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
)

type SearchAfter struct {
	Values []interface{}
}

func (p *SearchAfter) SetValues(values []interface{}) {
	p.Values = values
}

func (p *SearchAfter) ProtoBuffer() (*otsprotocol.SearchAfter, error) {
	pb := &otsprotocol.SearchAfter{}
	pb.Values = make([][]byte, 0)
	for _, v := range p.Values {
		vv, err := ToVariantValue(v)
		if err != nil {
			return nil, err
		}
		pb.Values = append(pb.Values, ([]byte)(vv))
	}
	return pb, nil
}
