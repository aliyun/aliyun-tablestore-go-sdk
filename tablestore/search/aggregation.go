package search

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

type AggregationType int

const (
	AggregationNoneType          AggregationType = 0
	AggregationAvgType           AggregationType = 1
	AggregationDistinctCountType AggregationType = 2
	AggregationMaxType           AggregationType = 3
	AggregationMinType           AggregationType = 4
	AggregationSumType           AggregationType = 5
	AggregationCountType         AggregationType = 6
)

func (a AggregationType) Enum() *AggregationType {
	newAgg := a
	return &newAgg
}

func (a AggregationType) String() string {
	switch a {
	case AggregationAvgType:
		return "avg"
	case AggregationDistinctCountType:
		return "distinct_count"
	case AggregationMaxType:
		return "max"
	case AggregationMinType:
		return "min"
	case AggregationSumType:
		return "sum"
	case AggregationCountType:
		return "count"
	default:
		return "unknown"
	}
}

func (a AggregationType) ToPB() *otsprotocol.AggregationType {
	switch a {
	case AggregationNoneType:
		return nil
	case AggregationAvgType:
		return otsprotocol.AggregationType_AGG_AVG.Enum()
	case AggregationDistinctCountType:
		return otsprotocol.AggregationType_AGG_DISTINCT_COUNT.Enum()
	case AggregationMaxType:
		return otsprotocol.AggregationType_AGG_MAX.Enum()
	case AggregationMinType:
		return otsprotocol.AggregationType_AGG_MIN.Enum()
	case AggregationSumType:
		return otsprotocol.AggregationType_AGG_SUM.Enum()
	case AggregationCountType:
		return otsprotocol.AggregationType_AGG_COUNT.Enum()
	default:
		panic("unexpected")
	}
}

/*
message Aggregation {
    optional string name = 1;
    optional AggregationType type = 2;
    optional bytes body = 3;
}
 */
type Aggregation interface {
	//get agg name
	GetName() string

	//get agg type
	GetType() AggregationType

	//build body, implemented by each concrete agg
	Serialize() ([]byte, error)

	// build the whole aggregation, implemented by each concrete agg,
	// using BuildPBForAggregation() defined in agg interface
	ProtoBuffer() (*otsprotocol.Aggregation, error)
}

func BuildPBForAggregation(a Aggregation) (*otsprotocol.Aggregation, error) {
	pbAggregation := &otsprotocol.Aggregation{}

	pbAggregation.Name = proto.String(a.GetName())
	pbAggregation.Type = a.GetType().ToPB()
	body, err := a.Serialize()
	if err != nil {
		return nil, err
	}
	pbAggregation.Body = body
	return pbAggregation, nil
}

func BuildPBForAggregations(aggs []Aggregation) (*otsprotocol.Aggregations, error) {
	if len(aggs) == 0 {
		return nil, nil
	}

	pbAggregations := new(otsprotocol.Aggregations)
	for _, subAgg := range aggs {
		pbAggregation, err := subAgg.ProtoBuffer()
		if err != nil {
			return nil, errors.New("invalid agg: " + err.Error())
		}
		pbAggregations.Aggs = append(pbAggregations.Aggs, pbAggregation)
	}
	return pbAggregations, nil
}
