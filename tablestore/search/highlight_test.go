package search

import (
    "encoding/json"
    "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
    "github.com/golang/protobuf/proto"
    "github.com/stretchr/testify/assert"
    "testing"
)

func TestNewHighlightParameter(t *testing.T) {
    highlightParameter := NewHighlightParameter()
    highlightParameter.SetHighlightFragmentOrder(Score)
    highlightParameter.SetFragmentSize(100)
    highlightParameter.SetNumberOfFragments(50)
    highlightParameter.SetPreTag("<em>")
    highlightParameter.SetPostTag("</em>")
    data, err := json.Marshal(highlightParameter)
    assert.Nil(t, err)
    assert.NotNil(t, data)
    
    // UnmarshalJSON
    parseHighlightParameter := &HighlightParameter{}
    err = json.Unmarshal(data, parseHighlightParameter)
    assert.Nil(t, err)
    assert.Equal(t, "<em>", *parseHighlightParameter.PreTag)
    assert.Equal(t, "</em>", *parseHighlightParameter.PostTag)
    assert.Equal(t, Score, *parseHighlightParameter.HighlightFragmentOrder)
    assert.Equal(t, int32(100), *parseHighlightParameter.FragmentSize)
    assert.Equal(t, int32(50), *parseHighlightParameter.NumberOfFragments)
}

func TestToHighlightParameter(t *testing.T) {
    pbHighlightParameter := &otsprotocol.HighlightParameter{}
    highlightParameter, err := ToHighlightParameter(pbHighlightParameter)
    assert.Nil(t, err)
    assert.Equal(t, TextSequence, *highlightParameter.HighlightFragmentOrder)
    assert.Nil(t, highlightParameter.PreTag)
    assert.Nil(t, highlightParameter.PostTag)
    assert.Nil(t, highlightParameter.NumberOfFragments)
    assert.Nil(t, highlightParameter.FragmentSize)
    
    pbHighlightParameter.PreTag = proto.String("<em>")
    pbHighlightParameter.PostTag = proto.String("</em>")
    pbHighlightParameter.FragmentSize = proto.Int32(100)
    pbHighlightParameter.NumberOfFragments = proto.Int32(50)
    pbHighlightParameter.FragmentsOrder = otsprotocol.HighlightFragmentOrder_SCORE.Enum()
    highlightParameter, err = ToHighlightParameter(pbHighlightParameter)
    assert.Nil(t, err)
    assert.Equal(t, Score, *highlightParameter.HighlightFragmentOrder)
    assert.Equal(t, "<em>", *highlightParameter.PreTag)
    assert.Equal(t, "</em>", *highlightParameter.PostTag)
    assert.Equal(t, int32(100), *highlightParameter.FragmentSize)
    assert.Equal(t, int32(50), *highlightParameter.NumberOfFragments)
}

func TestNewHighlight(t *testing.T) {
    highlight := NewHighlight()
    highlight.SetHighlightEncoder(HtmlMode)
    highlight.AddFieldHighlightParameter("highlight_field",
        NewHighlightParameter().
        SetHighlightFragmentOrder(Score).
        SetNumberOfFragments(50).
        SetFragmentSize(100).
        SetPreTag("<em>").
        SetPostTag("</em>"))
    data, err := json.Marshal(highlight)
    assert.Nil(t, err)
    
    parseHighlight := &Highlight{}
    err = json.Unmarshal(data, parseHighlight)
    assert.Nil(t, err)
    assert.Equal(t, HtmlMode, *parseHighlight.HighlightEncoder)
    assert.NotNil(t, parseHighlight.FieldHighlightParameters["highlight_field"])
    assert.Equal(t, Score, *(parseHighlight.FieldHighlightParameters["highlight_field"].HighlightFragmentOrder))
    assert.Equal(t, int32(100), *(parseHighlight.FieldHighlightParameters["highlight_field"].FragmentSize))
    assert.Equal(t, int32(50), *(parseHighlight.FieldHighlightParameters["highlight_field"].NumberOfFragments))
    assert.Equal(t, "<em>", *(parseHighlight.FieldHighlightParameters["highlight_field"].PreTag))
    assert.Equal(t, "</em>", *(parseHighlight.FieldHighlightParameters["highlight_field"].PostTag))
}

func TestHighlight_ToHighlight(t *testing.T) {
    pbHighlight := &otsprotocol.Highlight{}
    
    highlight, err := ToHighlight(pbHighlight)
    assert.Nil(t, err)
    assert.Equal(t, PlainMode, *highlight.HighlightEncoder)
    assert.Equal(t, 0, len(highlight.FieldHighlightParameters))
    
    pbHighlight.HighlightEncoder = otsprotocol.HighlightEncoder_HTML_MODE.Enum()
    pbHighlight.HighlightParameters = append([]*otsprotocol.HighlightParameter{}, &otsprotocol.HighlightParameter{
        FieldName: proto.String("highlight_field"),
        PreTag: proto.String("<b>"),
        PostTag: proto.String("</b>"),
        FragmentsOrder: otsprotocol.HighlightFragmentOrder_SCORE.Enum(),
        NumberOfFragments: proto.Int32(50),
        FragmentSize: proto.Int32(100),
    })
    
    highlight, err = ToHighlight(pbHighlight)
    assert.Nil(t, err)
    assert.Equal(t, HtmlMode, *highlight.HighlightEncoder)
    assert.NotNil(t, highlight.FieldHighlightParameters["highlight_field"])
    highlightParameter := highlight.FieldHighlightParameters["highlight_field"]
    assert.Equal(t, "<b>", *highlightParameter.PreTag)
    assert.Equal(t, "</b>", *highlightParameter.PostTag)
    assert.Equal(t, Score, *highlightParameter.HighlightFragmentOrder)
    assert.Equal(t, int32(50), *highlightParameter.NumberOfFragments)
    assert.Equal(t, int32(100), *highlightParameter.FragmentSize)
}

func TestToHighlightFragmentOrder(t *testing.T) {
    pbHighlightFragmentOrder := otsprotocol.HighlightFragmentOrder_TEXT_SEQUENCE
    highlightFragmentOrder, err := ToHighlightFragmentOrder(pbHighlightFragmentOrder.String())
    assert.Nil(t, err)
    assert.Equal(t, TextSequence, highlightFragmentOrder)
    pbHighlightFragmentOrder = otsprotocol.HighlightFragmentOrder_SCORE
    highlightFragmentOrder, err = ToHighlightFragmentOrder(pbHighlightFragmentOrder.String())
    assert.Nil(t, err)
    assert.Equal(t, Score, highlightFragmentOrder)
}

func TestToHighlightEncoder(t *testing.T) {
    pbHighlightEncoder := otsprotocol.HighlightEncoder_HTML_MODE
    highlightEncoder, err := ToHighlightEncoder(pbHighlightEncoder.String())
    assert.Nil(t, err)
    assert.Equal(t, HtmlMode, highlightEncoder)
    pbHighlightEncoder = otsprotocol.HighlightEncoder_PLAIN_MODE
    highlightEncoder, err = ToHighlightEncoder(pbHighlightEncoder.String())
    assert.Nil(t, err)
    assert.Equal(t, PlainMode, highlightEncoder)
}

func TestHighlight_UnmarshalJSON(t *testing.T) {
    highlight := &Highlight{
        HighlightEncoder: HtmlMode.Enum(),
        FieldHighlightParameters: map[string]*HighlightParameter{
            "highlight_field1": {
                NumberOfFragments:      proto.Int32(10),
                FragmentSize:           proto.Int32(50),
                HighlightFragmentOrder: TextSequence.Enum(),
                PreTag:                 proto.String("<em>"),
                PostTag:                proto.String("</em>"),
            },
            "highlight_field2": {
                NumberOfFragments:      proto.Int32(20),
                FragmentSize:           proto.Int32(100),
                HighlightFragmentOrder: Score.Enum(),
                PreTag:                 proto.String("<em>"),
                PostTag:                proto.String("</em>"),
            },
        },
    }
    
    highlightBytes, err := json.Marshal(highlight)
    assert.Nil(t, err)
    
    parsedHighlight := &Highlight{}
    err = json.Unmarshal(highlightBytes, parsedHighlight)
    assert.Nil(t, err)
    assert.Equal(t, highlight.HighlightEncoder, parsedHighlight.HighlightEncoder)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].HighlightFragmentOrder, parsedHighlight.FieldHighlightParameters["highlight_field1"].HighlightFragmentOrder)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].NumberOfFragments, parsedHighlight.FieldHighlightParameters["highlight_field1"].NumberOfFragments)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].FragmentSize, parsedHighlight.FieldHighlightParameters["highlight_field1"].FragmentSize)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].PreTag, parsedHighlight.FieldHighlightParameters["highlight_field1"].PreTag)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].PostTag, parsedHighlight.FieldHighlightParameters["highlight_field1"].PostTag)

    // 高亮参数json传入格式
    highlightJson := `{
        "HighlightEncoder": "html_mode",
        "FieldHighlightParameters": {
            "highlight_field1": {
                "NumberOfFragments": 10,
                "FragmentSize": 50,
                "PreTag": "<em>",
                "PostTag": "</em>",
                "HighlightFragmentOrder": "text_sequence"
            },
            "highlight_field2": {
                "NumberOfFragments": 20,
                "FragmentSize": 100,
                "PreTag": "<em>",
                "PostTag": "</em>",
                "HighlightFragmentOrder": "score"
            }
        }
    }`
    
    err = json.Unmarshal([]byte(highlightJson), parsedHighlight)
    assert.Nil(t, err)
    assert.Equal(t, highlight.HighlightEncoder, parsedHighlight.HighlightEncoder)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].HighlightFragmentOrder, parsedHighlight.FieldHighlightParameters["highlight_field1"].HighlightFragmentOrder)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].NumberOfFragments, parsedHighlight.FieldHighlightParameters["highlight_field1"].NumberOfFragments)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].FragmentSize, parsedHighlight.FieldHighlightParameters["highlight_field1"].FragmentSize)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].PreTag, parsedHighlight.FieldHighlightParameters["highlight_field1"].PreTag)
    assert.Equal(t, highlight.FieldHighlightParameters["highlight_field1"].PostTag, parsedHighlight.FieldHighlightParameters["highlight_field1"].PostTag)
}
