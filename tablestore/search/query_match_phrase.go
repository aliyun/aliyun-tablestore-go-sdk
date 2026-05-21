package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

type MatchPhraseQuery struct {
	FieldName string   // FieldName is the name of the field to be queried.
	Text      string   // Text is the phrase to be matched.
	Weight    *float32 // Weight is the weight of the query.
	Slop      *int32   // Slop is the maximum number of positions allowed between matching tokens for phrases.
}

func (q *MatchPhraseQuery) Type() QueryType {
	return QueryType_MatchPhraseQuery
}

func (q *MatchPhraseQuery) Serialize() ([]byte, error) {
	query := &otsprotocol.MatchPhraseQuery{}
	query.FieldName = &q.FieldName
	query.Text = &q.Text
	if q.Weight != nil {
		query.Weight = q.Weight
	}
	if q.Slop != nil {
		query.Slop = q.Slop
	}
	data, err := proto.Marshal(query)
	return data, err
}

func (q *MatchPhraseQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	return BuildPBForQuery(q)
}
