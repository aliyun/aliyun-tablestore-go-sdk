package search

import (
    "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
    "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search/model"
    "github.com/golang/protobuf/proto"
    "github.com/stretchr/testify/assert"
    "testing"
)

func Int32Ptr(i int32) *int32 {
    return &i
}

func TestFunctionsScoreQuery_SetQuery(t *testing.T) {
    functionsScoreQuery := NewFunctionsScoreQuery()
    functionsScoreQuery.SetQuery(&MatchAllQuery{})
    expectQuery := MatchAllQuery{}
    assert.Equal(t, expectQuery.Type(), functionsScoreQuery.Query.Type())
}

func TestFunctionsScoreQuery_SetFunctions(t *testing.T) {
    functionsScoreQuery := NewFunctionsScoreQuery()
    functions := []*ScoreFunction{
        NewScoreFunction().
            SetDecayFunction(NewDecayFunction().
                SetFieldName("Col_GeoPoint").
                SetMathFunction(GAUSS).
                SetDecayParam(NewDecayFuncGeoParam().
                    SetOrigin("30.137817,120.08681").
                    SetScale(1000).
                    SetOffset(0)).
                SetDecay(0.6).
                SetMultiValueMode(MVM_SUM)).
            SetFilter(&ExistsQuery{FieldName: "Col_GeoPoint"}).
            SetWeight(2),
        NewScoreFunction().
            SetRandomFunction(NewRandomFunction()),
        NewScoreFunction().
            SetFieldValueFactorFunction(NewFieldValueFactorFunction().
                SetFieldName("Col_Double").
                SetFactor(1.1).
                SetFunctionModifier(LN).
                SetMissing(1.0)),
    }
    functionsScoreQuery.SetFunctions(functions)
    assert.Equal(t, functions, functionsScoreQuery.Functions)
}

func TestFunctionsScoreQuery_AddFunction(t *testing.T) {
    functionsScoreQuery := NewFunctionsScoreQuery()
    functions := []*ScoreFunction{
        NewScoreFunction().
            SetDecayFunction(NewDecayFunction().
                SetFieldName("Col_GeoPoint").
                SetMathFunction(GAUSS).
                SetDecayParam(NewDecayFuncGeoParam().
                    SetOrigin("30.137817,120.08681").
                    SetScale(1000).
                    SetOffset(0)).
                SetDecay(0.6).
                SetMultiValueMode(MVM_SUM)).
            SetFilter(&ExistsQuery{FieldName: "Col_GeoPoint"}).
            SetWeight(2),
        NewScoreFunction().
            SetRandomFunction(NewRandomFunction()),
        NewScoreFunction().
            SetFieldValueFactorFunction(NewFieldValueFactorFunction().
                SetFieldName("Col_Double").
                SetFactor(1.1).
                SetFunctionModifier(LN).
                SetMissing(1.0)),
    }
    for i := 0; i < len(functions); i++ {
        functionsScoreQuery.AddFunction(functions[i])
    }
    assert.Equal(t, functions, functionsScoreQuery.Functions)
}

func TestFunctionsScoreQuery_SetScoreMode(t *testing.T) {
    for _, mode := range []ScoreMode{SM_AVG, SM_MAX, SM_MIN, SM_MULTIPLY, SM_SUM, SM_FIRST} {
        functionsScoreQuery := NewFunctionsScoreQuery()
        functionsScoreQuery.SetScoreMode(mode)
        assert.Equal(t, mode, *functionsScoreQuery.ScoreMode)
    }
}

func TestFunctionsScoreQuery_SetCombineMode(t *testing.T) {
    for _, mode := range []CombineMode{CM_MIN, CM_MAX, CM_MULTIPLY, CM_REPLACE, CM_SUM, CM_AVG} {
        functionsScoreQuery := NewFunctionsScoreQuery()
        functionsScoreQuery.SetCombineMode(mode)
        assert.Equal(t, mode, *functionsScoreQuery.CombineMode)
    }
}

func TestFunctionsScoreQuery_SetMinScore(t *testing.T) {
    for _, minScore := range []float32{0.1, 0.5, 0.9} {
        functionsScoreQuery := NewFunctionsScoreQuery()
        functionsScoreQuery.SetMinScore(minScore)
        assert.Equal(t, minScore, *functionsScoreQuery.MinScore)
    }
}

func TestFunctionsScoreQuery_SetMaxScore(t *testing.T) {
    for _, maxScore := range []float32{0.1, 0.5, 0.9} {
        functionsScoreQuery := NewFunctionsScoreQuery()
        functionsScoreQuery.SetMaxScore(maxScore)
        assert.Equal(t, maxScore, *functionsScoreQuery.MaxScore)
    }
}

func TestFunctionsScoreQuery_MarshalJSON(t *testing.T) {
    functionsScore := NewFunctionsScoreQuery().
        SetQuery(&MatchAllQuery{}).
        AddFunction(NewScoreFunction().
            SetDecayFunction(NewDecayFunction().
                SetFieldName("Col_GeoPoint").
                SetMathFunction(GAUSS).
                SetDecayParam(NewDecayFuncGeoParam().
                    SetOrigin("30.137817,120.08681").
                    SetScale(1000).
                    SetOffset(0)).
                SetDecay(0.6).
                SetMultiValueMode(MVM_SUM)).
            SetFilter(&ExistsQuery{FieldName: "Col_GeoPoint"}).
            SetWeight(2)).
        AddFunction(NewScoreFunction().
            SetRandomFunction(NewRandomFunction())).
        AddFunction(NewScoreFunction().
            SetFieldValueFactorFunction(NewFieldValueFactorFunction().
                SetFieldName("Col_Double").
                SetFactor(1.1).
                SetFunctionModifier(LN).
                SetMissing(1.0))).
        SetMaxScore(1000).
        SetMinScore(0).
        SetScoreMode(SM_MAX).
        SetCombineMode(CM_MAX)
    json, err := functionsScore.MarshalJSON()
    assert.Nil(t, err)
    expected := []byte(`{"Query":{"Name":"MatchAllQuery","Query":{}}}`)
    assert.Equal(t, expected, json)
}

func TestFunctionsScoreQuery_UnmarshalJSON(t *testing.T) {
    data := []byte(`{"Functions":[{"FieldValueFactorFunction":null,"DecayFunction":{"FieldName":"Col_GeoPoint","ParamType":"geo","DecayParam":{"Origin":"30.137817,120.08681","Scale":1000,"Offset":0},"MathFunction":0,"Decay":0.6,"MultiValueMode":2},"RandomFunction":null,"Weight":2,"Filter":{"FieldName":"Col_GeoPoint"}},{"FieldValueFactorFunction":null,"DecayFunction":null,"RandomFunction":{},"Weight":null,"Filter":null},{"FieldValueFactorFunction":{"FieldName":"Col_Double","Factor":1.1,"Modifier":4,"Missing":1},"DecayFunction":null,"RandomFunction":null,"Weight":null,"Filter":null}],"ScoreMode":1,"CombineMode":2,"MinScore":0,"MaxScore":1000,"Query":{"Name":"MatchAllQuery","Query":{}}}`)
    functionScore := NewFunctionsScoreQuery()
    err := functionScore.UnmarshalJSON(data)
    assert.Nil(t, err)
    expected := NewFunctionsScoreQuery().
        SetQuery(&MatchAllQuery{})
    assert.Equal(t, expected, functionScore)
}

func TestFunctionsScoreQuery_Type(t *testing.T) {
    functionsScoreQuery := NewFunctionsScoreQuery()
    assert.Equal(t, QueryType_FunctionsScoreQuery, functionsScoreQuery.Type())
}

func TestFunctionsScoreQuery_Serialize(t *testing.T) {
    bytes, err := NewFunctionsScoreQuery().
        SetQuery(&MatchAllQuery{}).
        AddFunction(NewScoreFunction().
            SetDecayFunction(NewDecayFunction().
                SetFieldName("Col_GeoPoint").
                SetMathFunction(GAUSS).
                SetDecayParam(NewDecayFuncGeoParam().
                    SetOrigin("30.137817,120.08681").
                    SetScale(1000).
                    SetOffset(0)).
                SetDecay(0.6).
                SetMultiValueMode(MVM_SUM)).
            SetFilter(&ExistsQuery{FieldName: "Col_GeoPoint"}).
            SetWeight(2)).
        AddFunction(NewScoreFunction().
            SetRandomFunction(NewRandomFunction())).
        AddFunction(NewScoreFunction().
            SetFieldValueFactorFunction(NewFieldValueFactorFunction().
                SetFieldName("Col_Double").
                SetFactor(1.1).
                SetFunctionModifier(LN).
                SetMissing(1.0))).
        SetMaxScore(1000).
        SetMinScore(0).
        SetScoreMode(SM_MAX).
        SetCombineMode(CM_MAX).Serialize()

    assert.Nil(t, err)
    var pb otsprotocol.FunctionsScoreQuery
    err2 := proto.Unmarshal(bytes, &pb)
    assert.Nil(t, err2)

    expectedFunction1, _ := NewScoreFunction().
        SetDecayFunction(NewDecayFunction().
            SetFieldName("Col_GeoPoint").
            SetMathFunction(GAUSS).
            SetDecayParam(NewDecayFuncGeoParam().
                SetOrigin("30.137817,120.08681").
                SetScale(1000).
                SetOffset(0)).
            SetDecay(0.6).
            SetMultiValueMode(MVM_SUM)).
        SetFilter(&ExistsQuery{FieldName: "Col_GeoPoint"}).
        SetWeight(2).ProtoBuffer()
    expectedFunction2, _ := NewScoreFunction().SetRandomFunction(NewRandomFunction()).ProtoBuffer()
    expectedFunction3, _ := NewScoreFunction().
        SetFieldValueFactorFunction(NewFieldValueFactorFunction().
            SetFieldName("Col_Double").
            SetFactor(1.1).
            SetFunctionModifier(LN).
            SetMissing(1.0)).ProtoBuffer()
    expectedFunctions := []*otsprotocol.Function{expectedFunction1, expectedFunction2, expectedFunction3}
    assert.Equal(t, expectedFunctions, pb.Functions)

    expectedScoreMode := otsprotocol.FunctionScoreMode_FSM_MAX
    assert.Equal(t, expectedScoreMode, *pb.SocreMode)

    expectedCombineMode := otsprotocol.FunctionCombineMode_FCM_MAX
    assert.Equal(t, expectedCombineMode, *pb.CombineMode)

    assert.Equal(t, float32(0), *pb.MinScore)
    assert.Equal(t, float32(1000), *pb.MaxScore)
}

func TestFunctionsScoreQuery_Serialize_WithoutQuery(t *testing.T) {
    _, err := NewFunctionsScoreQuery().
        AddFunction(NewScoreFunction().
            SetDecayFunction(NewDecayFunction().
                SetFieldName("Col_GeoPoint").
                SetMathFunction(GAUSS).
                SetDecayParam(NewDecayFuncGeoParam().
                    SetOrigin("30.137817,120.08681").
                    SetScale(1000).
                    SetOffset(0)).
                SetDecay(0.6).
                SetMultiValueMode(MVM_SUM)).
            SetFilter(&ExistsQuery{FieldName: "Col_GeoPoint"}).
            SetWeight(2)).
        AddFunction(NewScoreFunction().
            SetRandomFunction(NewRandomFunction())).
        AddFunction(NewScoreFunction().
            SetFieldValueFactorFunction(NewFieldValueFactorFunction().
                SetFieldName("Col_Double").
                SetFactor(1.1).
                SetFunctionModifier(LN).
                SetMissing(1.0))).
        SetMaxScore(1000).
        SetMinScore(0).
        SetScoreMode(SM_MAX).
        SetCombineMode(CM_MAX).Serialize()
    assert.Equal(t, "FunctionsScoreQuery: Query or Functions is nil", err.Error())
}

func TestFunctionsScoreQuery_Serialize_WithoutFunctions(t *testing.T) {
    _, err := NewFunctionsScoreQuery().
        SetQuery(&MatchAllQuery{}).
        SetMaxScore(1000).
        SetMinScore(0).
        SetScoreMode(SM_MAX).
        SetCombineMode(CM_MAX).Serialize()
    assert.Equal(t, "FunctionsScoreQuery: Query or Functions is nil", err.Error())
}

func TestFunctionsScoreQuery_ProtoBuffer(t *testing.T) {
    pb, err := NewFunctionsScoreQuery().
        SetQuery(&MatchAllQuery{}).
        AddFunction(NewScoreFunction().
            SetDecayFunction(NewDecayFunction().
                SetFieldName("Col_GeoPoint").
                SetMathFunction(GAUSS).
                SetDecayParam(NewDecayFuncGeoParam().
                    SetOrigin("30.137817,120.08681").
                    SetScale(1000).
                    SetOffset(0)).
                SetDecay(0.6).
                SetMultiValueMode(MVM_SUM)).
            SetFilter(&ExistsQuery{FieldName: "Col_GeoPoint"}).
            SetWeight(2)).
        AddFunction(NewScoreFunction().
            SetRandomFunction(NewRandomFunction())).
        AddFunction(NewScoreFunction().
            SetFieldValueFactorFunction(NewFieldValueFactorFunction().
                SetFieldName("Col_Double").
                SetFactor(1.1).
                SetFunctionModifier(LN).
                SetMissing(1.0))).
        SetMaxScore(1000).
        SetMinScore(0).
        SetScoreMode(SM_MAX).
        SetCombineMode(CM_MAX).ProtoBuffer()
    assert.Nil(t, err)
    assert.Equal(t, otsprotocol.QueryType_FUNCTIONS_SCORE_QUERY, *pb.Type)
}

func TestScoreFunction_SetFieldValueFactorFunction(t *testing.T) {
    function := NewScoreFunction()
    function.SetFieldValueFactorFunction(NewFieldValueFactorFunction().
        SetFieldName("Col_Double").
        SetFactor(1.1).
        SetFunctionModifier(LN).
        SetMissing(1.0))
    expected := NewFieldValueFactorFunction().
        SetFieldName("Col_Double").
        SetFactor(1.1).
        SetFunctionModifier(LN).
        SetMissing(1.0)
    assert.Equal(t, expected, function.FieldValueFactorFunction)
}

func TestScoreFunction_SetDecayFunction(t *testing.T) {
    function := NewScoreFunction()
    function.SetDecayFunction(NewDecayFunction().
        SetFieldName("Col_GeoPoint").
        SetMathFunction(GAUSS).
        SetDecayParam(NewDecayFuncGeoParam().
            SetOrigin("30.137817,120.08681").
            SetScale(1000).
            SetOffset(0)).
        SetDecay(0.6).
        SetMultiValueMode(MVM_SUM))
    expected := NewDecayFunction().
        SetFieldName("Col_GeoPoint").
        SetMathFunction(GAUSS).
        SetDecayParam(NewDecayFuncGeoParam().
            SetOrigin("30.137817,120.08681").
            SetScale(1000).
            SetOffset(0)).
        SetDecay(0.6).
        SetMultiValueMode(MVM_SUM)
    assert.Equal(t, expected, function.DecayFunction)
}

func TestScoreFunction_SetRandomFunction(t *testing.T) {
    function := NewScoreFunction()
    function.SetRandomFunction(NewRandomFunction())
    expected := NewRandomFunction()
    assert.Equal(t, expected, function.RandomFunction)
}

func TestScoreFunction_SetWeight(t *testing.T) {
    function := NewScoreFunction()
    function.SetWeight(1.1)
    assert.Equal(t, float32(1.1), *function.Weight)
}

func TestScoreFunction_SetFilter(t *testing.T) {
    function := NewScoreFunction()
    function.SetFilter(&MatchAllQuery{})
    expected := &MatchAllQuery{}
    assert.Equal(t, expected, function.Filter)
}

func TestScoreFunction_ProtoBuffer(t *testing.T) {
    expectedFieldName := "Col_GeoPoint"
    expectedDecayFunction := NewDecayFunction().
        SetFieldName(expectedFieldName).
        SetMathFunction(GAUSS).
        SetDecayParam(NewDecayFuncGeoParam().
            SetOrigin("30.137817,120.08681").
            SetScale(1000).
            SetOffset(0)).
        SetDecay(0.6).
        SetMultiValueMode(MVM_SUM)
    expectedFieldValueFactor := NewFieldValueFactorFunction().
        SetFieldName("Col_Double").
        SetFactor(1.1).
        SetFunctionModifier(LN).
        SetMissing(1.0)
    expectedRandomFunction := NewRandomFunction()
    expectedFilter := ExistsQuery{FieldName: expectedFieldName}
    expectedWeight := float32(2)
    function := NewScoreFunction()
    function.SetDecayFunction(expectedDecayFunction)
    function.SetFieldValueFactorFunction(expectedFieldValueFactor)
    function.SetRandomFunction(expectedRandomFunction)
    function.SetFilter(&expectedFilter)
    function.SetWeight(2)
    pb, err := function.ProtoBuffer()
    assert.Nil(t, err)
    expectedPbDecayFunction, _ := expectedDecayFunction.ProtoBuffer()
    expectedPbFieldValueFactor, _ := expectedFieldValueFactor.ProtoBuffer()
    expectedPbRandomFunction, _ := expectedRandomFunction.ProtoBuffer()
    expectedPbFilter, _ := expectedFilter.ProtoBuffer()
    expected := &otsprotocol.Function{
        Decay:            expectedPbDecayFunction,
        FieldValueFactor: expectedPbFieldValueFactor,
        Random:           expectedPbRandomFunction,
        Filter:           expectedPbFilter,
        Weight:           &expectedWeight,
    }
    assert.Equal(t, expected, pb)
}

func TestFieldValueFactorFunction_SetFieldName(t *testing.T) {
    function := NewFieldValueFactorFunction()
    function.SetFieldName("Col_Double")
    assert.Equal(t, "Col_Double", *function.FieldName)
}

func TestFieldValueFactorFunction_SetFactor(t *testing.T) {
    function := NewFieldValueFactorFunction()
    function.SetFactor(1.1)
    assert.Equal(t, float32(1.1), *function.Factor)
}

func TestFieldValueFactorFunction_SetFunctionModifier(t *testing.T) {
    function := NewFieldValueFactorFunction()
    function.SetFunctionModifier(LN)
    assert.Equal(t, LN, *function.Modifier)
}

func TestFieldValueFactorFunction_SetMissing(t *testing.T) {
    function := NewFieldValueFactorFunction()
    function.SetMissing(1.0)
    assert.Equal(t, 1.0, *function.Missing)
}

func TestFieldValueFactorFunction_ProtoBuffer(t *testing.T) {
    expectedFieldName := "Col_Double"
    expectedFactor := float32(1.1)
    expectedModifier := LN
    expectedMissing := 1.0
    function := NewFieldValueFactorFunction()
    function.SetFieldName(expectedFieldName)
    function.SetFactor(expectedFactor)
    function.SetFunctionModifier(expectedModifier)
    function.SetMissing(expectedMissing)
    pb, err := function.ProtoBuffer()
    assert.Nil(t, err)
    expected := &otsprotocol.FieldValueFactorFunction{
        FieldName: &expectedFieldName,
        Factor:    &expectedFactor,
        Modifier:  expectedModifier.ProtoBuffer(),
        Missing:   &expectedMissing,
    }
    assert.Equal(t, expected, pb)
}

func TestFunctionModifier_ProtoBuffer(t *testing.T) {
    for _, modifier := range []struct {
        input  FunctionModifier
        output otsprotocol.FunctionModifier
    }{
        {input: NONE, output: otsprotocol.FunctionModifier_FM_NONE},
        {input: LOG, output: otsprotocol.FunctionModifier_FM_LOG},
        {input: LOG1P, output: otsprotocol.FunctionModifier_FM_LOG1P},
        {input: LOG2P, output: otsprotocol.FunctionModifier_FM_LOG2P},
        {input: LN, output: otsprotocol.FunctionModifier_FM_LN},
        {input: LN1P, output: otsprotocol.FunctionModifier_FM_LN1P},
        {input: LN2P, output: otsprotocol.FunctionModifier_FM_LN2P},
        {input: SQUARE, output: otsprotocol.FunctionModifier_FM_SQUARE},
        {input: RECIPROCAL, output: otsprotocol.FunctionModifier_FM_RECIPROCAL},
        {input: SQRT, output: otsprotocol.FunctionModifier_FM_SQRT},
    } {
        assert.Equal(t, modifier.output, *modifier.input.ProtoBuffer())
    }
}

func TestFunctionModifier_Enum(t *testing.T) {
    for _, modifier := range []FunctionModifier{NONE, LOG, LOG1P, LOG2P, LN, LN1P, LN2P, SQUARE, RECIPROCAL, SQRT} {
        assert.Equal(t, modifier, *modifier.Enum())
    }
}

func TestDecayFunction_SetFieldName(t *testing.T) {
    function := NewDecayFunction()
    function.SetFieldName("Col_GeoPoint")
    assert.Equal(t, "Col_GeoPoint", *function.FieldName)
}

func TestDecayFunction_SetDecayParam(t *testing.T) {
    function := NewDecayFunction()
    expected := NewDecayFuncGeoParam().
        SetOrigin("30.137817,120.08681").
        SetScale(1000).
        SetOffset(0)
    function.SetDecayParam(expected)
    assert.Equal(t, expected, function.DecayParam)
    assert.Equal(t, expected.GetType(), function.ParamType)
}

func TestDecayFunction_SetMathFunction(t *testing.T) {
    function := NewDecayFunction()
    function.SetMathFunction(GAUSS)
    assert.Equal(t, GAUSS, *function.MathFunction)
}

func TestDecayFunction_SetDecay(t *testing.T) {
    function := NewDecayFunction()
    function.SetDecay(0.6)
    assert.Equal(t, 0.6, *function.Decay)
}

func TestDecayFunction_SetMultiValueMode(t *testing.T) {
    function := NewDecayFunction()
    function.SetMultiValueMode(MVM_SUM)
    assert.Equal(t, MVM_SUM, *function.MultiValueMode)
}

func TestDecayFunction_ProtoBuffer_DateParam(t *testing.T) {
    expectedFieldName := "Col_Date"
    expectedScale := model.DateTimeValue{Value: Int32Ptr(1), Unit: model.DateTimeUnit_DAY.Enum()}
    expectedOffset := model.DateTimeValue{Value: Int32Ptr(1), Unit: model.DateTimeUnit_HOUR.Enum()}
    expectedDecay := 0.6
    expectedDecayFunction := NewDecayFunction().
        SetFieldName(expectedFieldName).
        SetMathFunction(GAUSS).
        SetDecayParam(NewDecayFuncDateParam().
            SetOriginLong(1500000000000).
            SetScale(&expectedScale).
            SetOffset(&expectedOffset)).
        SetDecay(expectedDecay).
        SetMultiValueMode(MVM_SUM)
    pb, err := expectedDecayFunction.ProtoBuffer()
    assert.Nil(t, err)
    bytes, _ := proto.Marshal(&otsprotocol.DecayFuncDateParam{
        OriginLong: expectedDecayFunction.DecayParam.(*DecayFuncDateParam).OriginLong,
        Scale:      expectedDecayFunction.DecayParam.(*DecayFuncDateParam).Scale.ProtoBuffer(),
        Offset:     expectedDecayFunction.DecayParam.(*DecayFuncDateParam).Offset.ProtoBuffer(),
    })
    expected := &otsprotocol.DecayFunction{
        FieldName:      &expectedFieldName,
        Decay:          &expectedDecay,
        MultiValueMode: expectedDecayFunction.MultiValueMode.ProtoBuffer(),
        ParamType:      otsprotocol.DecayFuncParamType_DF_DATE_PARAM.Enum(),
        Param:          bytes,
        MathFunction:   otsprotocol.DecayMathFunction_GAUSS.Enum(),
    }
    assert.Equal(t, expected, pb)
}

func TestDecayFunction_ProtoBuffer_GeoParam(t *testing.T) {
    expectedFieldName := "Col_GeoPoint"
    expectedOrigin := "30.137817,120.08681"
    expectedScale := 1000.0
    expectedOffset := 0.0
    expectedDecay := 0.6
    expectedDecayFunction := NewDecayFunction().
        SetFieldName(expectedFieldName).
        SetMathFunction(GAUSS).
        SetDecayParam(NewDecayFuncGeoParam().
            SetOrigin(expectedOrigin).
            SetScale(expectedScale).
            SetOffset(expectedOffset)).
        SetDecay(expectedDecay).
        SetMultiValueMode(MVM_SUM)
    pb, err := expectedDecayFunction.ProtoBuffer()
    assert.Nil(t, err)
    bytes, _ := proto.Marshal(&otsprotocol.DecayFuncGeoParam{
        Origin: expectedDecayFunction.DecayParam.(*DecayFuncGeoParam).Origin,
        Scale:  &expectedScale,
        Offset: &expectedOffset,
    })
    expected := &otsprotocol.DecayFunction{
        FieldName:      &expectedFieldName,
        Decay:          &expectedDecay,
        MultiValueMode: expectedDecayFunction.MultiValueMode.ProtoBuffer(),
        ParamType:      otsprotocol.DecayFuncParamType_DF_GEO_PARAM.Enum(),
        Param:          bytes,
        MathFunction:   otsprotocol.DecayMathFunction_GAUSS.Enum(),
    }
    assert.Equal(t, expected, pb)
}

func TestDecayFunction_ProtoBuffer_NumericParam(t *testing.T) {
    expectedFieldName := "Col_Long"
    expectedOrigin := 1000.0
    expectedScale := 1000.0
    expectedOffset := 0.0
    expectedDecay := 0.6
    expectedDecayFunction := NewDecayFunction().
        SetFieldName(expectedFieldName).
        SetMathFunction(GAUSS).
        SetDecayParam(NewDecayFuncNumericParam().
            SetOrigin(expectedOrigin).
            SetScale(expectedScale).
            SetOffset(expectedOffset)).
        SetDecay(expectedDecay).
        SetMultiValueMode(MVM_SUM)
    pb, err := expectedDecayFunction.ProtoBuffer()
    assert.Nil(t, err)
    bytes, _ := proto.Marshal(&otsprotocol.DecayFuncNumericParam{
        Origin: &expectedOrigin,
        Scale:  &expectedScale,
        Offset: &expectedOffset,
    })
    expected := &otsprotocol.DecayFunction{
        FieldName:      &expectedFieldName,
        Decay:          &expectedDecay,
        MultiValueMode: expectedDecayFunction.MultiValueMode.ProtoBuffer(),
        ParamType:      otsprotocol.DecayFuncParamType_DF_NUMERIC_PARAM.Enum(),
        Param:          bytes,
        MathFunction:   otsprotocol.DecayMathFunction_GAUSS.Enum(),
    }
    assert.Equal(t, expected, pb)
}

func TestParamType_ProtoBuffer(t *testing.T) {
    for _, test := range []struct {
        input  ParamType
        output otsprotocol.DecayFuncParamType
    }{
        {PT_DATE, otsprotocol.DecayFuncParamType_DF_DATE_PARAM},
        {PT_GEO, otsprotocol.DecayFuncParamType_DF_GEO_PARAM},
        {PT_NUMERIC, otsprotocol.DecayFuncParamType_DF_NUMERIC_PARAM},
    } {
        assert.Equal(t, test.output, *test.input.ProtoBuffer())
    }
}

func TestParamType_Enum(t *testing.T)  {
    for _, paramType := range []ParamType{PT_DATE, PT_GEO, PT_NUMERIC} {
        assert.Equal(t, paramType, *paramType.Enum())
    }
}

func TestDecayFuncDateParam_SetOriginLong(t *testing.T) {
    for _, originLong := range []int64{1000000000000000, 2000000000000000, 5000000000000000} {
        decayFuncDateParam := NewDecayFuncDateParam()
        decayFuncDateParam.SetOriginLong(originLong)
        assert.Equal(t, originLong, *decayFuncDateParam.OriginLong)
    }
}

func TestDecayFuncDateParam_SetOriginString(t *testing.T) {
    for _, originString := range []string{"2023-11-27 10:55:21.000", "2023-11-27 10:55:20.000", "2023-11-27 10:55:19.000"} {
        decayFuncDateParam := NewDecayFuncDateParam()
        decayFuncDateParam.SetOriginString(originString)
        assert.Equal(t, originString, *decayFuncDateParam.OriginString)
    }
}

func TestDecayFuncDateParam_SetScale(t *testing.T) {
    for _, scale := range []model.DateTimeValue{
        {Value: Int32Ptr(1), Unit: model.DateTimeUnit_HOUR.Enum()},
        {Value: Int32Ptr(2), Unit: model.DateTimeUnit_DAY.Enum()},
        {Value: Int32Ptr(3), Unit: model.DateTimeUnit_MINUTE.Enum()},
    } {
        decayFuncDateParam := NewDecayFuncDateParam()
        decayFuncDateParam.SetScale(&scale)
        assert.Equal(t, scale, *decayFuncDateParam.Scale)
    }
}

func TestDecayFuncDateParam_SetOffset(t *testing.T) {
    for _, offset := range []model.DateTimeValue{
        {Value: Int32Ptr(1), Unit: model.DateTimeUnit_HOUR.Enum()},
        {Value: Int32Ptr(2), Unit: model.DateTimeUnit_DAY.Enum()},
        {Value: Int32Ptr(3), Unit: model.DateTimeUnit_MINUTE.Enum()},
    } {
        decayFuncDateParam := NewDecayFuncDateParam()
        decayFuncDateParam.SetOffset(&offset)
        assert.Equal(t, offset, *decayFuncDateParam.Offset)
    }
}

func TestDecayFuncDateParam_GetType(t *testing.T) {
    decayFuncDateParam := NewDecayFuncDateParam()
    assert.Equal(t, PT_DATE, decayFuncDateParam.GetType())
}

func TestDecayFuncDateParam_ProtoBuffer(t *testing.T) {
    expectedOriginLong := int64(1000000000000000)
    expectedOriginString := "2023-11-27 10:55:21.000"
    expectedScale := model.DateTimeValue{Value: Int32Ptr(1), Unit: model.DateTimeUnit_HOUR.Enum()}
    expectedOffset := model.DateTimeValue{Value: Int32Ptr(1), Unit: model.DateTimeUnit_MINUTE.Enum()}
    expectedParam := otsprotocol.DecayFuncDateParam{
        OriginLong:   &expectedOriginLong,
        OriginString: &expectedOriginString,
        Scale:        expectedScale.ProtoBuffer(),
        Offset:       expectedOffset.ProtoBuffer(),
    }
    param := DecayFuncDateParam{
        OriginLong:   &expectedOriginLong,
        OriginString: &expectedOriginString,
        Scale:        &expectedScale,
        Offset:       &expectedOffset,
    }
    actualParam, err := param.ProtoBuffer()
    assert.Nil(t, err)
    assert.Equal(t, expectedParam, *actualParam)
}

func TestDecayFuncGeoParam_SetOrigin(t *testing.T) {
    for _, origin := range []string{"1,1", "2,2", "3,3"} {
        decayFuncGeoParam := NewDecayFuncGeoParam()
        decayFuncGeoParam.SetOrigin(origin)
        assert.Equal(t, origin, *decayFuncGeoParam.Origin)
    }
}

func TestDecayFuncGeoParam_SetScale(t *testing.T) {
    for _, scale := range []float64{1.1, 2.2, 3.3} {
        decayFuncGeoParam := NewDecayFuncGeoParam()
        decayFuncGeoParam.SetScale(scale)
        assert.Equal(t, scale, *decayFuncGeoParam.Scale)
    }
}

func TestDecayFuncGeoParam_SetOffset(t *testing.T) {
    for _, offset := range []float64{1.1, 2.2, 3.3} {
        decayFuncGeoParam := NewDecayFuncGeoParam()
        decayFuncGeoParam.SetOffset(offset)
        assert.Equal(t, offset, *decayFuncGeoParam.Offset)
    }
}

func TestDecayFuncGeoParam_GetType(t *testing.T) {
    decayFuncGeoParam := NewDecayFuncGeoParam()
    assert.Equal(t, PT_GEO, decayFuncGeoParam.GetType())
}

func TestDecayFuncGeoParam_ProtoBuffer(t *testing.T) {
    expectedOrigin := "1,1"
    expectedScale := 1.1
    expectedOffset := 0.1
    expectedParam := otsprotocol.DecayFuncGeoParam{
        Origin: &expectedOrigin,
        Scale:  &expectedScale,
        Offset: &expectedOffset,
    }
    param := DecayFuncGeoParam{
        Origin: &expectedOrigin,
        Scale:  &expectedScale,
        Offset: &expectedOffset,
    }
    actualParam, err := param.ProtoBuffer()
    assert.Nil(t, err)
    assert.Equal(t, expectedParam, *actualParam)
}

func TestDecayFuncNumericParam_SetOrigin(t *testing.T) {
    for _, origin := range []float64{1.1, 2.2, 3.3} {
        decayFuncNumericParam := NewDecayFuncNumericParam()
        decayFuncNumericParam.SetOrigin(origin)
        assert.Equal(t, origin, *decayFuncNumericParam.Origin)
    }
}

func TestDecayFuncNumericParam_SetScale(t *testing.T) {
    for _, scale := range []float64{1.1, 2.2, 3.3} {
        decayFuncNumericParam := NewDecayFuncNumericParam()
        decayFuncNumericParam.SetScale(scale)
        assert.Equal(t, scale, *decayFuncNumericParam.Scale)
    }
}

func TestDecayFuncNumericParam_SetOffset(t *testing.T) {
    for _, offset := range []float64{1.1, 2.2, 3.3} {
        decayFuncNumericParam := NewDecayFuncNumericParam()
        decayFuncNumericParam.SetOffset(offset)
        assert.Equal(t, offset, *decayFuncNumericParam.Offset)
    }
}

func TestDecayFuncNumericParam_GetType(t *testing.T) {
    decayFuncNumericParam := NewDecayFuncNumericParam()
    assert.Equal(t, PT_NUMERIC, decayFuncNumericParam.GetType())
}

func TestDecayFuncNumericParam_ProtoBuffer(t *testing.T) {
    expectedOrigin := 1.1
    expectedScale := 1.1
    expectedOffset := 0.1
    expectedParam := otsprotocol.DecayFuncNumericParam{
        Origin: &expectedOrigin,
        Scale:  &expectedScale,
        Offset: &expectedOffset,
    }
    param := DecayFuncNumericParam{
        Origin: &expectedOrigin,
        Scale:  &expectedScale,
        Offset: &expectedOffset,
    }
    actualParam, err := param.ProtoBuffer()
    assert.Nil(t, err)
    assert.Equal(t, expectedParam, *actualParam)
}

func TestMathFunction_ProtoBuffer(t *testing.T) {
    for _, mathFunction := range []struct {
        input  MathFunction
        output otsprotocol.DecayMathFunction
    }{
        {input: GAUSS, output: otsprotocol.DecayMathFunction_GAUSS},
        {input: EXP, output: otsprotocol.DecayMathFunction_EXP},
        {input: LINEAR, output: otsprotocol.DecayMathFunction_LINEAR},
    } {
        actualOutput := mathFunction.input.ProtoBuffer()
        assert.Equal(t, mathFunction.output, *actualOutput)
    }
}

func TestMathFunction_Enum(t *testing.T) {
    for _, mathFunction := range []MathFunction{GAUSS, EXP, LINEAR} {
        assert.Equal(t, mathFunction, *mathFunction.Enum())
    }
}

func TestMultiValueMode_ProtoBuffer(t *testing.T) {
    for _, multiValueMode := range []struct {
        input  MultiValueMode
        output otsprotocol.MultiValueMode
    }{
        {input: MVM_AVG, output: otsprotocol.MultiValueMode_MVM_AVG},
        {input: MVM_MAX, output: otsprotocol.MultiValueMode_MVM_MAX},
        {input: MVM_MIN, output: otsprotocol.MultiValueMode_MVM_MIN},
        {input: MVM_SUM, output: otsprotocol.MultiValueMode_MVM_SUM},
    } {
        actualOutput := multiValueMode.input.ProtoBuffer()
        assert.Equal(t, multiValueMode.output, *actualOutput)
    }
}

func TestMultiValueMode_Enum(t *testing.T) {
    for _, multiValueMode := range []MultiValueMode{MVM_AVG, MVM_MAX, MVM_MIN, MVM_SUM} {
        assert.Equal(t, multiValueMode, *multiValueMode.Enum())
    }
}

func TestRandomFunction_ProtoBuffer(t *testing.T) {
    expected := otsprotocol.RandomScoreFunction{}
    random := RandomFunction{}
    actual, err := random.ProtoBuffer()
    assert.Nil(t, err)
    assert.Equal(t, expected, *actual)
}

func TestScoreMode_Enum(t *testing.T) {
    for _, scoreMode := range []ScoreMode{SM_AVG, SM_MAX, SM_MIN, SM_SUM, SM_MULTIPLY, SM_FIRST} {
        assert.Equal(t, scoreMode, *scoreMode.Enum())
    }
}

func TestScoreMode_ProtoBuffer(t *testing.T) {
    for _, scoreMode := range []struct {
        input  ScoreMode
        output otsprotocol.FunctionScoreMode
    }{
        {input: SM_AVG, output: otsprotocol.FunctionScoreMode_FSM_AVG},
        {input: SM_MAX, output: otsprotocol.FunctionScoreMode_FSM_MAX},
        {input: SM_MIN, output: otsprotocol.FunctionScoreMode_FSM_MIN},
        {input: SM_SUM, output: otsprotocol.FunctionScoreMode_FSM_SUM},
        {input: SM_MULTIPLY, output: otsprotocol.FunctionScoreMode_FSM_MULTIPLY},
        {input: SM_FIRST, output: otsprotocol.FunctionScoreMode_FSM_FIRST},
    } {
        actualOutput := scoreMode.input.ProtoBuffer()
        assert.Equal(t, scoreMode.output, *actualOutput)
    }
}

func TestCombineMode_Enum(t *testing.T) {
    for _, combineMode := range []CombineMode{CM_MULTIPLY, CM_AVG, CM_MAX, CM_SUM, CM_MIN, CM_REPLACE} {
        assert.Equal(t, combineMode, *combineMode.Enum())
    }
}

func TestCombineMode_ProtoBuffer(t *testing.T) {
    for _, combineMode := range []struct {
        input  CombineMode
        output otsprotocol.FunctionCombineMode
    }{
        {CM_MULTIPLY, otsprotocol.FunctionCombineMode_FCM_MULTIPLY},
        {CM_AVG, otsprotocol.FunctionCombineMode_FCM_AVG},
        {CM_MAX, otsprotocol.FunctionCombineMode_FCM_MAX},
        {CM_SUM, otsprotocol.FunctionCombineMode_FCM_SUM},
        {CM_MIN, otsprotocol.FunctionCombineMode_FCM_MIN},
        {CM_REPLACE, otsprotocol.FunctionCombineMode_FCM_REPLACE},
    } {
        assert.Equal(t, combineMode.output, *combineMode.input.ProtoBuffer())
    }
}

