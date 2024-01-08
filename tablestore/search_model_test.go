package tablestore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

// ConvertFieldSchemaToPBFieldSchema

func TestConvertFieldSchemaToPBFieldSchema_SingleWord(t *testing.T) {
	analyzer := Analyzer_SingleWord
	analyzerParam := SingleWordAnalyzerParameter{
		CaseSensitive: proto.Bool(true),
		DelimitWord:   proto.Bool(true),
	}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			Analyzer:          &analyzer,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbAnalyzerParamExpected := &otsprotocol.SingleWordAnalyzerParameter{
		CaseSensitive: proto.Bool(true),
		DelimitWord:   proto.Bool(true),
	}
	bytesAnalyzerParamExpected, _ := proto.Marshal(pbAnalyzerParamExpected)

	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("single_word")
	pbSchemaExpected.AnalyzerParameter = bytesAnalyzerParamExpected

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, pbSchemaExpected.AnalyzerParameter, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_SingleWord_NoDelimitWord(t *testing.T) {
	analyzer := Analyzer_SingleWord
	analyzerParam := SingleWordAnalyzerParameter{
		CaseSensitive: proto.Bool(true),
	}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			Analyzer:          &analyzer,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbAnalyzerParamExpected := &otsprotocol.SingleWordAnalyzerParameter{
		CaseSensitive: proto.Bool(true),
	}
	bytesAnalyzerParamExpected, _ := proto.Marshal(pbAnalyzerParamExpected)

	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("single_word")
	pbSchemaExpected.AnalyzerParameter = bytesAnalyzerParamExpected

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, pbSchemaExpected.AnalyzerParameter, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_SingleWord_NoCaseSensitive(t *testing.T) {
	analyzer := Analyzer_SingleWord
	analyzerParam := SingleWordAnalyzerParameter{
		DelimitWord: proto.Bool(true),
	}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			Analyzer:          &analyzer,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbAnalyzerParamExpected := &otsprotocol.SingleWordAnalyzerParameter{
		DelimitWord: proto.Bool(true),
	}
	bytesAnalyzerParamExpected, _ := proto.Marshal(pbAnalyzerParamExpected)

	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("single_word")
	pbSchemaExpected.AnalyzerParameter = bytesAnalyzerParamExpected

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, pbSchemaExpected.AnalyzerParameter, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_Split(t *testing.T) {
	analyzer := Analyzer_Split
	analyzerParam := SplitAnalyzerParameter{Delimiter: proto.String("-")}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			Analyzer:          &analyzer,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbAnalyzerParamExpected := &otsprotocol.SplitAnalyzerParameter{
		Delimiter: proto.String("-"),
	}
	bytesAnalyzerParamExpected, _ := proto.Marshal(pbAnalyzerParamExpected)

	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("split")
	pbSchemaExpected.AnalyzerParameter = bytesAnalyzerParamExpected

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, pbSchemaExpected.AnalyzerParameter, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_Split_NoDelimiter(t *testing.T) {
	analyzer := Analyzer_Split
	analyzerParam := SplitAnalyzerParameter{}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			Analyzer:          &analyzer,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbAnalyzerParamExpected := &otsprotocol.SplitAnalyzerParameter{}
	bytesAnalyzerParamExpected, _ := proto.Marshal(pbAnalyzerParamExpected)

	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("split")
	pbSchemaExpected.AnalyzerParameter = bytesAnalyzerParamExpected

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, pbSchemaExpected.AnalyzerParameter, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_Fuzzy(t *testing.T) {
	analyzer := Analyzer_Fuzzy
	analyzerParam := FuzzyAnalyzerParameter{
		MinChars: 2,
		MaxChars: 3,
	}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			Analyzer:          &analyzer,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbAnalyzerParamExpected := &otsprotocol.FuzzyAnalyzerParameter{
		MinChars: proto.Int32(2),
		MaxChars: proto.Int32(3),
	}
	bytesAnalyzerParamExpected, _ := proto.Marshal(pbAnalyzerParamExpected)

	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("fuzzy")
	pbSchemaExpected.AnalyzerParameter = bytesAnalyzerParamExpected

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, pbSchemaExpected.AnalyzerParameter, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_Fuzzy_NoMinChars(t *testing.T) {
	analyzer := Analyzer_Fuzzy
	analyzerParam := FuzzyAnalyzerParameter{
		MaxChars: 3,
	}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			Analyzer:          &analyzer,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbAnalyzerParamExpected := &otsprotocol.FuzzyAnalyzerParameter{
		MaxChars: proto.Int32(3),
	}
	bytesAnalyzerParamExpected, _ := proto.Marshal(pbAnalyzerParamExpected)

	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("fuzzy")
	pbSchemaExpected.AnalyzerParameter = bytesAnalyzerParamExpected

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, pbSchemaExpected.AnalyzerParameter, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_Fuzzy_NoMaxChars(t *testing.T) {
	analyzer := Analyzer_Fuzzy
	analyzerParam := FuzzyAnalyzerParameter{
		MaxChars: 3,
	}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			Analyzer:          &analyzer,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbAnalyzerParamExpected := &otsprotocol.FuzzyAnalyzerParameter{
		MaxChars: proto.Int32(3),
	}
	bytesAnalyzerParamExpected, _ := proto.Marshal(pbAnalyzerParamExpected)

	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("fuzzy")
	pbSchemaExpected.AnalyzerParameter = bytesAnalyzerParamExpected

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, pbSchemaExpected.AnalyzerParameter, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_MinWord(t *testing.T) {
	analyzer := Analyzer_MinWord

	schemas := []*FieldSchema{
		{
			FieldName: proto.String("Col_Analyzer"),
			FieldType: FieldType_TEXT,
			Analyzer:  &analyzer,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("min_word")

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, []byte(nil), pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_MaxWord(t *testing.T) {
	analyzer := Analyzer_MaxWord

	schemas := []*FieldSchema{
		{
			FieldName: proto.String("Col_Analyzer"),
			FieldType: FieldType_TEXT,
			Analyzer:  &analyzer,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("max_word")

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, []byte(nil), pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_SingleWordNoParam(t *testing.T) {
	analyzer := Analyzer_SingleWord
	schemas := []*FieldSchema{
		{
			FieldName: proto.String("Col_Analyzer"),
			FieldType: FieldType_TEXT,
			Analyzer:  &analyzer,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbSchemaExpected.Analyzer = proto.String("single_word")

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Equal(t, *pbSchemaExpected.Analyzer, *pbSchemas[0].Analyzer)
	assert.Equal(t, []byte(nil), pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_NoAnalyzerWithParam(t *testing.T) {
	analyzerParam := SingleWordAnalyzerParameter{CaseSensitive: proto.Bool(true)}
	schemas := []*FieldSchema{
		{
			FieldName:         proto.String("Col_Analyzer"),
			FieldType:         FieldType_TEXT,
			AnalyzerParameter: analyzerParam,
		},
	}

	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// expect result
	pbSchemaExpected := new(otsprotocol.FieldSchema)
	pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
	pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()

	// assert
	t.Log("pb actural ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 1)

	assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
	assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
	assert.Nil(t, pbSchemas[0].Analyzer)
	assert.Nil(t, pbSchemas[0].AnalyzerParameter)
}

func TestConvertFieldSchemaToPBFieldSchema_VirtualField(t *testing.T) {
	{
		analyzerParam := SingleWordAnalyzerParameter{CaseSensitive: proto.Bool(true)}
		schemas := []*FieldSchema{
			{
				FieldName:         proto.String("Col_Analyzer"),
				FieldType:         FieldType_TEXT,
				AnalyzerParameter: analyzerParam,
			},
		}

		// convert to pb
		var pbSchemas []*otsprotocol.FieldSchema
		pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

		// expect result
		pbSchemaExpected := new(otsprotocol.FieldSchema)
		pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
		pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()

		// assert
		t.Log("pb actural ==> ", pbSchemas)
		assert.Equal(t, len(pbSchemas), 1)

		assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
		assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
		assert.Nil(t, pbSchemas[0].Analyzer)
		assert.Nil(t, pbSchemas[0].AnalyzerParameter)
	}
	{
		analyzerParam := SingleWordAnalyzerParameter{CaseSensitive: proto.Bool(true)}
		schemas := []*FieldSchema{
			{
				FieldName:         proto.String("Col_Analyzer"),
				FieldType:         FieldType_TEXT,
				AnalyzerParameter: analyzerParam,
				IsVirtualField:    proto.Bool(true),
			},
		}

		// convert to pb
		var pbSchemas []*otsprotocol.FieldSchema
		pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

		// expect result
		pbSchemaExpected := new(otsprotocol.FieldSchema)
		pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
		pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
		pbSchemaExpected.IsVirtualField = proto.Bool(true)
		pbSchemaExpected.SourceFieldNames = []string{"sourceField"}

		// assert
		t.Log("pb actural ==> ", pbSchemas)
		assert.Equal(t, len(pbSchemas), 1)

		assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
		assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
		assert.Equal(t, *pbSchemaExpected.IsVirtualField, *pbSchemas[0].IsVirtualField)
		assert.NotEqual(t, pbSchemaExpected.SourceFieldNames, pbSchemas[0].SourceFieldNames)
		assert.Nil(t, pbSchemas[0].Analyzer)
		assert.Nil(t, pbSchemas[0].AnalyzerParameter)
	}
	{
		analyzerParam := SingleWordAnalyzerParameter{CaseSensitive: proto.Bool(true)}
		schemas := []*FieldSchema{
			{
				FieldName:         proto.String("Col_Analyzer"),
				FieldType:         FieldType_TEXT,
				AnalyzerParameter: analyzerParam,
				IsVirtualField:    proto.Bool(false),
			},
		}

		// convert to pb
		var pbSchemas []*otsprotocol.FieldSchema
		pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

		// expect result
		pbSchemaExpected := new(otsprotocol.FieldSchema)
		pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
		pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
		pbSchemaExpected.IsVirtualField = proto.Bool(false)
		pbSchemaExpected.SourceFieldNames = []string{"sourceField"}

		// assert
		t.Log("pb actural ==> ", pbSchemas)
		assert.Equal(t, len(pbSchemas), 1)

		assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
		assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
		assert.Equal(t, *pbSchemaExpected.IsVirtualField, *pbSchemas[0].IsVirtualField)
		assert.NotEqual(t, pbSchemaExpected.SourceFieldNames, pbSchemas[0].SourceFieldNames)
		assert.Nil(t, pbSchemas[0].Analyzer)
		assert.Nil(t, pbSchemas[0].AnalyzerParameter)
	}
	{
		analyzerParam := SingleWordAnalyzerParameter{CaseSensitive: proto.Bool(true)}
		schemas := []*FieldSchema{
			{
				FieldName:         proto.String("Col_Analyzer"),
				FieldType:         FieldType_TEXT,
				AnalyzerParameter: analyzerParam,
				IsVirtualField:    proto.Bool(true),
				SourceFieldNames:  []string{"sourceField"},
			},
		}

		// convert to pb
		var pbSchemas []*otsprotocol.FieldSchema
		pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

		// expect result
		pbSchemaExpected := new(otsprotocol.FieldSchema)
		pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
		pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
		pbSchemaExpected.IsVirtualField = proto.Bool(true)
		pbSchemaExpected.SourceFieldNames = []string{"sourceField"}

		// assert
		t.Log("pb actural ==> ", pbSchemas)
		assert.Equal(t, len(pbSchemas), 1)

		assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
		assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
		assert.Equal(t, *pbSchemaExpected.IsVirtualField, *pbSchemas[0].IsVirtualField)
		assert.Equal(t, pbSchemaExpected.SourceFieldNames, pbSchemas[0].SourceFieldNames)
		assert.Nil(t, pbSchemas[0].Analyzer)
		assert.Nil(t, pbSchemas[0].AnalyzerParameter)
	}
	{
		analyzerParam := SingleWordAnalyzerParameter{CaseSensitive: proto.Bool(true)}
		schemas := []*FieldSchema{
			{
				FieldName:         proto.String("Col_Analyzer"),
				FieldType:         FieldType_TEXT,
				AnalyzerParameter: analyzerParam,
				IsVirtualField:    proto.Bool(true),
				SourceFieldNames:  []string{"sourceField"},
			},
		}

		// convert to pb
		var pbSchemas []*otsprotocol.FieldSchema
		pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

		// expect result
		pbSchemaExpected := new(otsprotocol.FieldSchema)
		pbSchemaExpected.FieldName = proto.String("Col_Analyzer")
		pbSchemaExpected.FieldType = otsprotocol.FieldType_TEXT.Enum()
		pbSchemaExpected.IsVirtualField = proto.Bool(true)
		pbSchemaExpected.SourceFieldNames = []string{"sourceField"}

		// assert
		t.Log("pb actural ==> ", pbSchemas)
		assert.Equal(t, len(pbSchemas), 1)

		assert.Equal(t, *pbSchemaExpected.FieldName, *pbSchemas[0].FieldName)
		assert.Equal(t, *pbSchemaExpected.FieldType, *pbSchemas[0].FieldType)
		assert.Equal(t, *pbSchemaExpected.IsVirtualField, *pbSchemas[0].IsVirtualField)
		assert.Equal(t, pbSchemaExpected.SourceFieldNames, pbSchemas[0].SourceFieldNames)
		assert.Nil(t, pbSchemas[0].Analyzer)
		assert.Nil(t, pbSchemas[0].AnalyzerParameter)
	}
}

func TestConvertFieldSchemaToPBFieldSchema_Date(t *testing.T) {
	schemas := []*FieldSchema{
		{
			FieldName:   proto.String("date"),
			FieldType:   FieldType_DATE,
			DateFormats: []string{"format1", "format2"},
		},
		{
			FieldName: proto.String("nested"),
			FieldType: FieldType_NESTED,
			FieldSchemas: []*FieldSchema{
				{
					FieldName: proto.String("nested_date"),
					FieldType: FieldType_DATE,
				},
			},
		},
	}
	// convert to pb
	var pbSchemas []*otsprotocol.FieldSchema
	pbSchemas = convertFieldSchemaToPBFieldSchema(schemas)

	// assert
	t.Log("pb actual ==> ", pbSchemas)
	assert.Equal(t, len(pbSchemas), 2)

	assert.Equal(t, "date", *pbSchemas[0].FieldName)
	assert.Equal(t, *otsprotocol.FieldType_DATE.Enum(), *pbSchemas[0].FieldType)
	assert.Equal(t, []string{"format1", "format2"}, pbSchemas[0].DateFormats)

	assert.Equal(t, len(pbSchemas[1].FieldSchemas), 1)
	assert.Equal(t, "nested_date", *pbSchemas[1].FieldSchemas[0].FieldName)
	assert.Equal(t, *otsprotocol.FieldType_DATE.Enum(), *pbSchemas[1].FieldSchemas[0].FieldType)
	assert.Equal(t, 0, len(pbSchemas[1].FieldSchemas[0].DateFormats))
}

// parseFieldSchemaFromPb

func TestParseFieldSchemaFromPb_SingleWord(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.SingleWordAnalyzerParameter)
	pbParam.CaseSensitive = proto.Bool(true)
	pbParam.DelimitWord = proto.Bool(true)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("single_word")
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_SingleWord
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)

	assert.Equal(t, true, *(fieldSchemas[0].AnalyzerParameter).(SingleWordAnalyzerParameter).CaseSensitive)
}

func TestParseFieldSchemaFromPb_SingleWord_NoDelimitWord(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.SingleWordAnalyzerParameter)
	pbParam.CaseSensitive = proto.Bool(true)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("single_word")
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_SingleWord
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)

	assert.Equal(t, true, *(fieldSchemas[0].AnalyzerParameter).(SingleWordAnalyzerParameter).CaseSensitive)
	assert.Nil(t, (fieldSchemas[0].AnalyzerParameter).(SingleWordAnalyzerParameter).DelimitWord)
}

func TestParseFieldSchemaFromPb_SingleWord_NoCaseSensitive(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.SingleWordAnalyzerParameter)
	pbParam.DelimitWord = proto.Bool(true)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("single_word")
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_SingleWord
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)

	assert.Nil(t, (fieldSchemas[0].AnalyzerParameter).(SingleWordAnalyzerParameter).CaseSensitive)
	assert.Equal(t, true, *(fieldSchemas[0].AnalyzerParameter).(SingleWordAnalyzerParameter).DelimitWord)
}

func TestParseFieldSchemaFromPb_Split(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.SplitAnalyzerParameter)
	pbParam.Delimiter = proto.String("-")
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("split")
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_Split
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)

	assert.Equal(t, "-", *(fieldSchemas[0].AnalyzerParameter).(SplitAnalyzerParameter).Delimiter)
}

func TestParseFieldSchemaFromPb_Split_NoDelimiter(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.SplitAnalyzerParameter)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("split")
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_Split
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)

	assert.Nil(t, (fieldSchemas[0].AnalyzerParameter).(SplitAnalyzerParameter).Delimiter)
}

func TestParseFieldSchemaFromPb_Fuzzy(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.FuzzyAnalyzerParameter)
	pbParam.MinChars = proto.Int32(2)
	pbParam.MaxChars = proto.Int32(3)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("fuzzy")
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_Fuzzy
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)

	assert.Equal(t, int32(2), (fieldSchemas[0].AnalyzerParameter).(FuzzyAnalyzerParameter).MinChars)
	assert.Equal(t, int32(3), (fieldSchemas[0].AnalyzerParameter).(FuzzyAnalyzerParameter).MaxChars)
}

func TestParseFieldSchemaFromPb_Fuzzy_NoMinChars(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.FuzzyAnalyzerParameter)
	pbParam.MaxChars = proto.Int32(3)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("fuzzy")
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_Fuzzy
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)

	assert.Equal(t, int32(0), (fieldSchemas[0].AnalyzerParameter).(FuzzyAnalyzerParameter).MinChars)
	assert.Equal(t, int32(3), (fieldSchemas[0].AnalyzerParameter).(FuzzyAnalyzerParameter).MaxChars)
}

func TestParseFieldSchemaFromPb_Fuzzy_NoMaxChars(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.FuzzyAnalyzerParameter)
	pbParam.MinChars = proto.Int32(2)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("fuzzy")
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_Fuzzy
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)

	assert.Equal(t, int32(2), (fieldSchemas[0].AnalyzerParameter).(FuzzyAnalyzerParameter).MinChars)
	assert.Equal(t, int32(0), (fieldSchemas[0].AnalyzerParameter).(FuzzyAnalyzerParameter).MaxChars)
}

func TestParseFieldSchemaFromPb_MinWord(t *testing.T) {
	// build pb
	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("min_word")

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_MinWord
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)
	assert.Equal(t, nil, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_MaxWord(t *testing.T) {
	// build pb
	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("max_word")

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_MaxWord
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)
	assert.Equal(t, nil, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_SingleWordNoParam(t *testing.T) {
	// build pb
	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("single_word")

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_SingleWord
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)
	assert.Equal(t, nil, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_SplitNoParam(t *testing.T) {
	// build pb
	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("split")

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_Split
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)
	assert.Equal(t, nil, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_FuzzyNoParam(t *testing.T) {
	// build pb
	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.Analyzer = proto.String("fuzzy")

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	analyzerExpected := Analyzer_Fuzzy
	assert.Equal(t, analyzerExpected, *fieldSchemas[0].Analyzer)
	assert.Equal(t, nil, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_NoAnalyzerWithParam(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.SingleWordAnalyzerParameter)
	pbParam.CaseSensitive = proto.Bool(true)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	assert.Nil(t, fieldSchemas[0].Analyzer)
	assert.Nil(t, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_NoAnalyzerWithParam2(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.SplitAnalyzerParameter)
	//pbParam.Delimiter = proto.String("-")
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	assert.Nil(t, fieldSchemas[0].Analyzer)
	assert.Nil(t, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_NoAnalyzerWithParam3(t *testing.T) {
	// build pb
	pbParam := new(otsprotocol.FuzzyAnalyzerParameter)
	pbParam.MinChars = proto.Int32(2)
	pbParam.MaxChars = proto.Int32(3)
	pbParamBytes, _ := proto.Marshal(pbParam)

	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
	pbFieldSchema.AnalyzerParameter = pbParamBytes

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	assert.Nil(t, fieldSchemas[0].Analyzer)
	assert.Nil(t, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_NoAnalyzerNoParam(t *testing.T) {
	// build pb
	pbFieldSchema := new(otsprotocol.FieldSchema)
	pbFieldSchema.FieldName = proto.String("Col_Analyzer")
	pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()

	pbFieldSchemas := []*otsprotocol.FieldSchema{
		pbFieldSchema,
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 1)

	assert.Nil(t, fieldSchemas[0].Analyzer)
	assert.Nil(t, fieldSchemas[0].AnalyzerParameter)
}

func TestParseFieldSchemaFromPb_VirtualField(t *testing.T) {
	{
		// build pb
		pbFieldSchema := new(otsprotocol.FieldSchema)
		pbFieldSchema.FieldName = proto.String("Col_Analyzer")
		pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()

		pbFieldSchemas := []*otsprotocol.FieldSchema{
			pbFieldSchema,
		}

		// pb -> model
		fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

		// assert
		t.Log("fieldSchemas ==> ", fieldSchemas)
		assert.Equal(t, len(fieldSchemas), 1)

		assert.Nil(t, fieldSchemas[0].Analyzer)
		assert.Nil(t, fieldSchemas[0].AnalyzerParameter)
	}
	{
		// build pb
		pbFieldSchema := new(otsprotocol.FieldSchema)
		pbFieldSchema.FieldName = proto.String("Col_Analyzer")
		pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
		pbFieldSchema.IsVirtualField = proto.Bool(true)

		pbFieldSchemas := []*otsprotocol.FieldSchema{
			pbFieldSchema,
		}

		// pb -> model
		fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

		// assert
		t.Log("fieldSchemas ==> ", fieldSchemas)
		assert.Equal(t, len(fieldSchemas), 1)

		assert.Nil(t, fieldSchemas[0].Analyzer)
		assert.Nil(t, fieldSchemas[0].AnalyzerParameter)
		assert.Equal(t, *fieldSchemas[0].IsVirtualField, true)
	}
	{
		// build pb
		pbFieldSchema := new(otsprotocol.FieldSchema)
		pbFieldSchema.FieldName = proto.String("Col_Analyzer")
		pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
		pbFieldSchema.IsVirtualField = proto.Bool(false)

		pbFieldSchemas := []*otsprotocol.FieldSchema{
			pbFieldSchema,
		}

		// pb -> model
		fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

		// assert
		t.Log("fieldSchemas ==> ", fieldSchemas)
		assert.Equal(t, len(fieldSchemas), 1)

		assert.Nil(t, fieldSchemas[0].Analyzer)
		assert.Nil(t, fieldSchemas[0].AnalyzerParameter)
		assert.Equal(t, *fieldSchemas[0].IsVirtualField, false)
	}
	{
		// build pb
		pbFieldSchema := new(otsprotocol.FieldSchema)
		pbFieldSchema.FieldName = proto.String("Col_Analyzer")
		pbFieldSchema.FieldType = otsprotocol.FieldType_TEXT.Enum()
		pbFieldSchema.IsVirtualField = proto.Bool(false)
		pbFieldSchema.SourceFieldNames = []string{"sourceField"}

		pbFieldSchemas := []*otsprotocol.FieldSchema{
			pbFieldSchema,
		}

		// pb -> model
		fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

		// assert
		t.Log("fieldSchemas ==> ", fieldSchemas)
		assert.Equal(t, len(fieldSchemas), 1)

		assert.Nil(t, fieldSchemas[0].Analyzer)
		assert.Nil(t, fieldSchemas[0].AnalyzerParameter)
		assert.Equal(t, *fieldSchemas[0].IsVirtualField, false)
		assert.Equal(t, fieldSchemas[0].SourceFieldNames, []string{"sourceField"})
	}
}

func TestParseFieldSchemaFromPb_Date(t *testing.T) {
	// build pb
	pbFieldSchemas := []*otsprotocol.FieldSchema{
		{
			FieldName:   proto.String("date"),
			FieldType:   otsprotocol.FieldType_DATE.Enum(),
			DateFormats: []string{"format1", "format2"},
		},
		{
			FieldName: proto.String("date2"),
			FieldType: otsprotocol.FieldType_DATE.Enum(),
		},
	}

	// pb -> model
	fieldSchemas := parseFieldSchemaFromPb(pbFieldSchemas)

	// assert
	t.Log("fieldSchemas ==> ", fieldSchemas)
	assert.Equal(t, len(fieldSchemas), 2)

	assert.Equal(t, "date", *fieldSchemas[0].FieldName)
	assert.Equal(t, FieldType_DATE, fieldSchemas[0].FieldType)
	assert.Equal(t, []string{"format1", "format2"}, fieldSchemas[0].DateFormats)

	assert.Equal(t, "date2", *fieldSchemas[1].FieldName)
	assert.Equal(t, FieldType_DATE, fieldSchemas[1].FieldType)
	assert.Equal(t, 0, len(fieldSchemas[1].DateFormats))
}

func TestSearchQuery_LimitN1(t *testing.T) {
	// set to -1
	query := search.NewSearchQuery().
		SetQuery(&search.MatchAllQuery{}).
		SetLimit(-1)

	searchQueryBytes, err := query.Serialize()
	assert.Nil(t, err)

	pbSearchQuery := &otsprotocol.SearchQuery{}
	err = proto.Unmarshal(searchQueryBytes, pbSearchQuery)
	assert.Nil(t, err)
	assert.Equal(t, int32(-1), pbSearchQuery.GetLimit())

	// default to -2
	query = search.NewSearchQuery().SetQuery(&search.MatchAllQuery{})
	searchQueryBytes, err = query.Serialize()
	assert.Nil(t, err)

	pbSearchQuery = &otsprotocol.SearchQuery{}
	err = proto.Unmarshal(searchQueryBytes, pbSearchQuery)
	assert.Nil(t, err)
	assert.Nil(t, pbSearchQuery.Limit)
}

func TestSearchRequest_ProtoBuffer_TimeoutMs(t *testing.T) {
	//nil by default
	query := search.NewSearchQuery().SetQuery(&search.MatchAllQuery{})
	request := SearchRequest{
		SearchQuery: query,
	}
	pbSearchRequest, err := request.ProtoBuffer()
	assert.Nil(t, err)

	assert.Nil(t, pbSearchRequest.TimeoutMs)

	//set timeout_ms explicitly
	query = search.NewSearchQuery().SetQuery(&search.MatchAllQuery{})
	request = SearchRequest{
		SearchQuery: query,
		TimeoutMs:   proto.Int32(33),
	}
	pbSearchRequest, err = request.ProtoBuffer()
	assert.Nil(t, err)

	assert.Equal(t, int32(33), *pbSearchRequest.TimeoutMs)
}

func TestParallelScanRequest_ProtoBuffer(t *testing.T) {
	query := search.NewScanQuery().SetQuery(&search.MatchAllQuery{})
	request := ParallelScanRequest{
		TableName: "table1",
		IndexName: "index1",
		ScanQuery: query,
		ColumnsToGet: &ColumnsToGet{
			Columns:            []string{"col1", "col2"},
			ReturnAll:          false,
			ReturnAllFromIndex: false,
		},
		SessionId: []byte("bcd"),
	}

	pbParallelScanRequest, err := request.ProtoBuffer()
	assert.Nil(t, err)

	assert.Equal(t, "table1", *pbParallelScanRequest.TableName)
	assert.Equal(t, "index1", *pbParallelScanRequest.IndexName)
	assert.Nil(t, pbParallelScanRequest.TimeoutMs)

	//assert ScanQuery
	scanQueryExpected := &otsprotocol.ScanQuery{}
	scanQueryExpected.Query = &otsprotocol.Query{}
	scanQueryExpected.Query.Type = otsprotocol.QueryType_MATCH_ALL_QUERY.Enum()
	scanQueryExpected.Query.Query, _ = proto.Marshal(&otsprotocol.MatchAllQuery{})
	scanQueryBytesExpected, _ := proto.Marshal(scanQueryExpected)

	assert.Equal(t, scanQueryBytesExpected, pbParallelScanRequest.ScanQuery)

	//assert columnsToGet
	columnsToGetExpected := otsprotocol.ColumnsToGet{
		ReturnType:  otsprotocol.ColumnReturnType_RETURN_SPECIFIED.Enum(),
		ColumnNames: []string{"col1", "col2"},
	}
	assert.Equal(t, columnsToGetExpected, *pbParallelScanRequest.ColumnsToGet)

	assert.Equal(t, []byte("bcd"), pbParallelScanRequest.SessionId)
}

func TestParallelScanRequest_ProtoBuffer_TimeoutMs(t *testing.T) {
	//nil by default
	query := search.NewScanQuery().SetQuery(&search.MatchAllQuery{})
	request := ParallelScanRequest{
		ScanQuery: query,
	}
	pbParallelScanRequest, err := request.ProtoBuffer()
	assert.Nil(t, err)

	assert.Nil(t, pbParallelScanRequest.TimeoutMs)

	//set timeout_ms explicitly
	query = search.NewScanQuery().SetQuery(&search.MatchAllQuery{})
	request = ParallelScanRequest{
		ScanQuery: query,
		TimeoutMs: proto.Int32(33),
	}
	pbParallelScanRequest, err = request.ProtoBuffer()
	assert.Nil(t, err)

	assert.Equal(t, int32(33), *pbParallelScanRequest.TimeoutMs)
}

func Test_buildHighlightResult(t *testing.T) {
	type args struct {
		pbHighlightResult *otsprotocol.HighlightResult
	}

	tests := []struct {
		name string
		args args
		want *HighlightResultItem
	}{
		{
			name: "empty highlight result",
			args: args{
				pbHighlightResult: &otsprotocol.HighlightResult{},
			},
			want: &HighlightResultItem{
				HighlightFields: map[string]*HighlightField{},
			},
		},
		{
			name: "multi highlight fragment",
			args: args{
				pbHighlightResult: &otsprotocol.HighlightResult{
					HighlightFields: []*otsprotocol.HighlightField{
						{
							FieldName:      proto.String("col0"),
							FieldFragments: []string{"fragment1", "fragment2"},
						},
						{
							FieldName:      proto.String("col1"),
							FieldFragments: []string{"fragment1", "fragment2"},
						},
					},
				},
			},
			want: &HighlightResultItem{
				HighlightFields: map[string]*HighlightField{
					"col0": {
						Fragments: []string{"fragment1", "fragment2"},
					},
					"col1": {
						Fragments: []string{"fragment1", "fragment2"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, buildHighlightResult(tt.args.pbHighlightResult), "buildHighlightResult(%v)", tt.args.pbHighlightResult)
		})
	}
}

func Test_buildSearchHit(t *testing.T) {
	type args struct {
		pbSearchHit *otsprotocol.SearchHit
		row         *Row
	}
	tests := []struct {
		name string
		args args
		want *SearchHit
	}{
		{
			name: "empty search inner hits",
			args: args{
				pbSearchHit: &otsprotocol.SearchHit{
					HighlightResult: &otsprotocol.HighlightResult{
						HighlightFields: []*otsprotocol.HighlightField{
							{
								FieldName:      proto.String("col1"),
								FieldFragments: []string{"fragment1", "fragment2"},
							},
						},
					},
				},
			},
			want: &SearchHit{
				HighlightResultItem: &HighlightResultItem{
					HighlightFields: map[string]*HighlightField{
						"col1": {
							Fragments: []string{"fragment1", "fragment2"},
						},
					},
				},
				SearchInnerHits: map[string]*SearchInnerHit{},
			},
		},
		{
			name: "search inner hits",
			args: args{
				pbSearchHit: &otsprotocol.SearchHit{
					HighlightResult: &otsprotocol.HighlightResult{
						HighlightFields: []*otsprotocol.HighlightField{
							{
								FieldName:      proto.String("col1"),
								FieldFragments: []string{"fragment1", "fragment2"},
							},
						},
					},
					SearchInnerHits: []*otsprotocol.SearchInnerHit{
						{
							Path: proto.String("nested"),
							SearchHits: []*otsprotocol.SearchHit{
								{
									NestedDocOffset:    proto.Int32(0),
									Score: proto.Float64(math.MaxFloat64),
									HighlightResult: &otsprotocol.HighlightResult{
										HighlightFields: []*otsprotocol.HighlightField{
											{
												FieldName:      proto.String("nested.nested_col1"),
												FieldFragments: []string{"fragment1", "fragment2"},
											},
										},
									},
								},
							},
						},
						{
							Path: proto.String("nested1"),
							SearchHits: []*otsprotocol.SearchHit{
								{
									NestedDocOffset:    proto.Int32(0),
									Score: proto.Float64(math.MaxFloat64),
									HighlightResult: &otsprotocol.HighlightResult{
										HighlightFields: []*otsprotocol.HighlightField{
											{
												FieldName:      proto.String("nested1.nested1_col1"),
												FieldFragments: []string{"fragment1", "fragment2"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			want: &SearchHit{
				HighlightResultItem: &HighlightResultItem{
					HighlightFields: map[string]*HighlightField{
						"col1": {
							Fragments: []string{"fragment1", "fragment2"},
						},
					},
				},
				SearchInnerHits: map[string]*SearchInnerHit{
					"nested": {
						Path: "nested",
						SearchHits: []*SearchHit{
							{
								NestedDocOffset:   proto.Int32(0),
								Score: proto.Float64(math.MaxFloat64),
								HighlightResultItem: &HighlightResultItem{
									HighlightFields: map[string]*HighlightField{
										"nested.nested_col1": {
											Fragments: []string{"fragment1", "fragment2"},
										},
									},
								},
								SearchInnerHits: map[string]*SearchInnerHit{},
							},
						},
					},
					"nested1": {
						Path: "nested1",
						SearchHits: []*SearchHit{
							{
								NestedDocOffset:    proto.Int32(0),
								Score: proto.Float64(math.MaxFloat64),
								HighlightResultItem: &HighlightResultItem{
									HighlightFields: map[string]*HighlightField{
										"nested1.nested1_col1": {
											Fragments: []string{"fragment1", "fragment2"},
										},
									},
								},
								SearchInnerHits: map[string]*SearchInnerHit{},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, buildSearchHit(tt.args.pbSearchHit, tt.args.row), "buildSearchHit(%v, %v)", tt.args.pbSearchHit, tt.args.row)
		})
	}
}