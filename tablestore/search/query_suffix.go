package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

type SuffixQuery struct {
	FieldName string
	Suffix    string
	Weight    *float32
}

func (q *SuffixQuery) Type() QueryType {
	return QueryType_SuffixQuery
}

func (q *SuffixQuery) Serialize() ([]byte, error) {
	query := &otsprotocol.SuffixQuery{}
	query.FieldName = &q.FieldName
	query.Suffix = &q.Suffix
	if q.Weight != nil {
		query.Weight = q.Weight
	} else {
		query.Weight = proto.Float32(1.0)
	}
	data, err := proto.Marshal(query)
	return data, err
}

func (q *SuffixQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	return BuildPBForQuery(q)
}
