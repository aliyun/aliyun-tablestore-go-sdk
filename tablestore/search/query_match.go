package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

type MatchQuery struct {
	FieldName string
	Text      string
}

func (q *MatchQuery) Type() QueryType {
	return QueryType_MatchQuery
}

func (q *MatchQuery) Serialize() ([]byte, error) {
	query := &otsprotocol.MatchQuery{}
	query.FieldName = &q.FieldName
	query.Text = &q.Text
	data, err := proto.Marshal(query)
	return data, err
}

func (q *MatchQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	return BuildPBForQuery(q)
}
