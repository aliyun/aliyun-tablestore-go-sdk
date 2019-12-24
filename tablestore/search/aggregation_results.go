package search

import (
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

type AggregationResult interface {
	GetName() string
	GetType() AggregationType
}

type AggregationResults struct {
	resultMap map[string]AggregationResult
}

func (a *AggregationResults) GetRawResults() map[string]AggregationResult {
	m := make(map[string]AggregationResult, len(a.resultMap))
	for k, v := range a.resultMap {
		m[k] = v
	}
	return m
}

func (a *AggregationResults) Put(name string, result AggregationResult) {
	if a.resultMap == nil {
		a.resultMap = make(map[string]AggregationResult)
	}
	a.resultMap[name] = result
}

func (a AggregationResults) Avg(name string) (*AvgAggregationResult, error){
	if result, ok := a.resultMap[name]; ok {
		if result.GetType() != AggregationAvgType {
			return nil, errors.New(fmt.Sprintf("wrong agg type: [%v] needed, [%v] provided", result.GetType().String(), AggregationAvgType.String()))
		}
		return result.(*AvgAggregationResult), nil
	}
	return nil, errors.New(fmt.Sprintf("agg [%v] not found", name))
}

func (a AggregationResults) DistinctCount(name string) (*DistinctCountAggregationResult, error){
	if result, ok := a.resultMap[name]; ok {
		if result.GetType() != AggregationDistinctCountType {
			return nil, errors.New(fmt.Sprintf("wrong agg type: [%v] needed, [%v] provided", result.GetType().String(), AggregationDistinctCountType.String()))
		}
		return result.(*DistinctCountAggregationResult), nil
	}
	return nil, errors.New(fmt.Sprintf("agg [%v] not found", name))
}

func (a AggregationResults) Max(name string) (*MaxAggregationResult, error){
	if result, ok := a.resultMap[name]; ok {
		if result.GetType() != AggregationMaxType {
			return nil, errors.New(fmt.Sprintf("wrong agg type: [%v] needed, [%v] provided", result.GetType().String(), AggregationMaxType.String()))
		}
		return result.(*MaxAggregationResult), nil
	}
	return nil, errors.New(fmt.Sprintf("agg [%v] not found", name))
}

func (a AggregationResults) Min(name string) (*MinAggregationResult, error){
	if result, ok := a.resultMap[name]; ok {
		if result.GetType() != AggregationMinType {
			return nil, errors.New(fmt.Sprintf("wrong agg type: [%v] needed, [%v] provided", result.GetType().String(), AggregationMinType.String()))
		}
		return result.(*MinAggregationResult), nil
	}
	return nil, errors.New(fmt.Sprintf("agg [%v] not found", name))
}

func (a AggregationResults) Sum(name string) (*SumAggregationResult, error){
	if result, ok := a.resultMap[name]; ok {
		if result.GetType() != AggregationSumType {
			return nil, errors.New(fmt.Sprintf("wrong agg type: [%v] needed, [%v] provided", result.GetType().String(), AggregationSumType.String()))
		}
		return result.(*SumAggregationResult), nil
	}
	return nil, errors.New(fmt.Sprintf("agg [%v] not found", name))
}

func (a AggregationResults) Count(name string) (*CountAggregationResult, error){
	if result, ok := a.resultMap[name]; ok {
		if result.GetType() != AggregationCountType {
			return nil, errors.New(fmt.Sprintf("wrong agg type: [%v] needed, [%v] provided", result.GetType().String(), AggregationCountType.String()))
		}
		return result.(*CountAggregationResult), nil
	}
	return nil, errors.New(fmt.Sprintf("agg [%v] not found", name))
}

func (a AggregationResults) Empty() bool {
	return len(a.resultMap) == 0
}


func ParseAvgAggregationResultFromPB(pbAggResult *otsprotocol.AggregationResult) (*AvgAggregationResult, error) {
	aggResult := new(AvgAggregationResult)
	aggResult.Name = *pbAggResult.Name
	pbAggResultBody := new(otsprotocol.AvgAggregationResult)

	if err := proto.Unmarshal(pbAggResult.AggResult, pbAggResultBody); err != nil {
		return nil, err
	}
	if pbAggResultBody == nil || pbAggResultBody.Value == nil {
		return nil, errors.New("parse pb error")
	}
	aggResult.Value = *pbAggResultBody.Value
	return aggResult, nil
}

func ParseDistinctCountAggregationResultFromPB(pbAggResult *otsprotocol.AggregationResult) (*DistinctCountAggregationResult, error) {
	aggResult := new(DistinctCountAggregationResult)
	aggResult.Name = *pbAggResult.Name
	pbAggResultBody := new(otsprotocol.DistinctCountAggregationResult)

	if err := proto.Unmarshal(pbAggResult.AggResult, pbAggResultBody); err != nil {
		return nil, err
	}

	if pbAggResultBody == nil || pbAggResultBody.Value == nil {
		return nil, errors.New("parse pb error")
	}
	aggResult.Value = *pbAggResultBody.Value
	return aggResult, nil
}

func ParseMaxAggregationResultFromPB(pbAggResult *otsprotocol.AggregationResult) (*MaxAggregationResult, error) {
	aggResult := new(MaxAggregationResult)
	aggResult.Name = *pbAggResult.Name
	pbAggResultBody := new(otsprotocol.MaxAggregationResult)

	if err := proto.Unmarshal(pbAggResult.AggResult, pbAggResultBody); err != nil {
		return nil, err
	}
	if pbAggResultBody == nil || pbAggResultBody.Value == nil {
		return nil, errors.New("parse pb error")
	}
	aggResult.Value = *pbAggResultBody.Value
	return aggResult, nil
}

func ParseMinAggregationResultFromPB(pbAggResult *otsprotocol.AggregationResult) (*MinAggregationResult, error) {
	aggResult := new(MinAggregationResult)
	aggResult.Name = *pbAggResult.Name
	pbAggResultBody := new(otsprotocol.MinAggregationResult)

	if err := proto.Unmarshal(pbAggResult.AggResult, pbAggResultBody); err != nil {
		return nil, err
	}
	if pbAggResultBody == nil || pbAggResultBody.Value == nil {
		return nil, errors.New("parse pb error")
	}
	aggResult.Value = *pbAggResultBody.Value
	return aggResult, nil
}

func ParseSumAggregationResultFromPB(pbAggResult *otsprotocol.AggregationResult) (*SumAggregationResult, error) {
	aggResult := new(SumAggregationResult)
	aggResult.Name = *pbAggResult.Name
	pbAggResultBody := new(otsprotocol.SumAggregationResult)

	if err := proto.Unmarshal(pbAggResult.AggResult, pbAggResultBody); err != nil {
		return nil, err
	}
	if pbAggResultBody == nil || pbAggResultBody.Value == nil {
		return nil, errors.New("parse pb error")
	}
	aggResult.Value = *pbAggResultBody.Value
	return aggResult, nil
}

func ParseCountAggregationResultFromPB(pbAggResult *otsprotocol.AggregationResult) (*CountAggregationResult, error) {
	aggResult := new(CountAggregationResult)
	aggResult.Name = *pbAggResult.Name
	pbAggResultBody := new(otsprotocol.CountAggregationResult)

	if err := proto.Unmarshal(pbAggResult.AggResult, pbAggResultBody); err != nil {
		return nil, err
	}
	if pbAggResultBody == nil || pbAggResultBody.Value == nil {
		return nil, errors.New("parse pb error")
	}
	aggResult.Value = *pbAggResultBody.Value
	return aggResult, nil
}

func ParseAggregationResultsFromPB(pbAggregationResults []*otsprotocol.AggregationResult) (*AggregationResults, error) {
	aggregationResults := AggregationResults{}

	for _, pbAggResult := range pbAggregationResults {
		switch pbAggResult.GetType() {
		case otsprotocol.AggregationType_AGG_AVG:
			aggResult, err := ParseAvgAggregationResultFromPB(pbAggResult)
			if err != nil {
				return nil, err
			}
			aggregationResults.Put(aggResult.Name, aggResult)
			break
		case otsprotocol.AggregationType_AGG_DISTINCT_COUNT:
			aggResult, err := ParseDistinctCountAggregationResultFromPB(pbAggResult)
			if err != nil {
				return nil, err
			}
			aggregationResults.Put(aggResult.Name, aggResult)
			break
		case otsprotocol.AggregationType_AGG_MAX:
			aggResult, err := ParseMaxAggregationResultFromPB(pbAggResult)
			if err != nil {
				return nil, err
			}
			aggregationResults.Put(aggResult.Name, aggResult)
			break
		case otsprotocol.AggregationType_AGG_MIN:
			aggResult, err := ParseMinAggregationResultFromPB(pbAggResult)
			if err != nil {
				return nil, err
			}
			aggregationResults.Put(aggResult.Name, aggResult)
			break
		case otsprotocol.AggregationType_AGG_SUM:
			aggResult, err := ParseSumAggregationResultFromPB(pbAggResult)
			if err != nil {
				return nil, err
			}
			aggregationResults.Put(aggResult.Name, aggResult)
			break
		case otsprotocol.AggregationType_AGG_COUNT:
			aggResult, err := ParseCountAggregationResultFromPB(pbAggResult)
			if err != nil {
				return nil, err
			}
			aggregationResults.Put(aggResult.Name, aggResult)
			break
		default:
			return nil, errors.New(fmt.Sprintf("unknown agg result type: %v", pbAggResult.GetType()))
		}
	}
	return &aggregationResults, nil
}
