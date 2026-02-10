package search

import (
	"encoding/json"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

type DisMaxQuery struct {
	Queries    []Query `json:"-"`
	TieBreaker *float32
	Weight     *float32

	// for json marshal and unmarshal
	QueriesAlias []queryAlias `json:"Queries"`
}

func (q *DisMaxQuery) MarshalJSON() ([]byte, error) {
	type DisMaxQueryAlias DisMaxQuery
	disMaxQueryAlias := DisMaxQueryAlias(*q)
	if q.Queries != nil {
		qs := make([]queryAlias, 0)
		for _, tq := range q.Queries {
			qs = append(qs, queryAlias{
				Name:  tq.Type().String(),
				Query: tq,
			})
		}
		disMaxQueryAlias.QueriesAlias = qs
	}
	disMaxQueryAlias.TieBreaker = q.TieBreaker
	disMaxQueryAlias.Weight = q.Weight

	data, err := json.Marshal(disMaxQueryAlias)
	return data, err
}

func (q *DisMaxQuery) UnmarshalJSON(data []byte) (err error) {
	type DisMaxQueryAlias DisMaxQuery
	disMaxQueryAlias := &DisMaxQueryAlias{}
	err = json.Unmarshal(data, disMaxQueryAlias)
	if err != nil {
		return
	}

	q.TieBreaker = disMaxQueryAlias.TieBreaker
	q.Weight = disMaxQueryAlias.Weight
	if disMaxQueryAlias.QueriesAlias != nil {
		queries := make([]Query, 0)
		for _, tq := range disMaxQueryAlias.QueriesAlias {
			queries = append(queries, tq.Query)
		}
		q.Queries = queries
	}
	return
}

func (q *DisMaxQuery) Type() QueryType {
	return QueryType_DisMaxQuery
}

func (q *DisMaxQuery) Serialize() ([]byte, error) {
	query := &otsprotocol.DisMaxQuery{}
	if q.Queries != nil {
		pbQueries := make([]*otsprotocol.Query, 0)
		for _, subQuery := range q.Queries {
			pbSubQuery, err := subQuery.ProtoBuffer()
			if err != nil {
				return nil, err
			}
			pbQueries = append(pbQueries, pbSubQuery)
		}
		query.Queries = pbQueries
	}
	if q.TieBreaker != nil {
		query.TieBreaker = q.TieBreaker
	}
	if q.Weight != nil {
		query.Weight = q.Weight
	}
	data, err := proto.Marshal(query)
	return data, err
}

func (q *DisMaxQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	return BuildPBForQuery(q)
}
