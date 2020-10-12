package tunnel

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestSequenceInfo_FuzzSerializationCompare(t *testing.T) {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 10000; i++ {
		seqA := randomSeqInfo(random)
		seqB := randomSeqInfo(random)
		checkCompareConsistence(t, seqA, seqB)
		checkCompareConsistence(t, seqB, seqA)
	}
}

func checkCompareConsistence(t *testing.T, l, r *SequenceInfo) {
	comp := l.Serialization() < r.Serialization()
	if structComp := StreamRecordSequenceLess(l, r); comp != structComp {
		t.Errorf("l %v %s r %v %s, want %t, got %t", l, l.Serialization(),
			r, r.Serialization(), structComp, comp)
	}
}

func randomSeqInfo(random *rand.Rand) *SequenceInfo {
	epoch := random.Int31n(10000)
	timestamp := random.Int63n(time.Now().UnixNano() / 1000)
	index := random.Int31()
	return &SequenceInfo{
		Epoch:     epoch,
		Timestamp: timestamp,
		RowIndex:  index,
	}
}

func TestParseSerializedSeqInfo(t *testing.T) {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	randSeq := randomSeqInfo(random)

	type args struct {
		hexedSeqStr string
	}
	tests := []struct {
		name    string
		args    args
		want    *SequenceInfo
		wantErr bool
	}{
		{
			"valid seq string",
			args{randSeq.Serialization()},
			randSeq,
			false,
		},
		{
			"invalid seq string 1",
			args{fmt.Sprintf("%08x", 1234)},
			nil,
			true,
		},
		{
			"invalid seq string 2",
			args{"fff:xxx:sss"},
			nil,
			true,
		},
		{
			"invalid seq string 3",
			args{"a&b:bbb:ccc"},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSerializedSeqInfo(tt.args.hexedSeqStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSerializedSeqInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseSerializedSeqInfo() got = %v, want %v", got, tt.want)
			}
		})
	}
}
