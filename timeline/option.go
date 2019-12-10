package timeline

import (
	"fmt"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/timeline/writer"
)

var (
	DefaultFirstPk  = "TimelineId"
	DefaultSecondPk = "Sequence"

	MinTTL = 86400
)

type StoreOption struct {
	Endpoint      string
	Instance      string
	TableName     string
	AkId          string
	AkSecret      string
	SecurityToken string

	Schema     *Schema
	TTL        int
	Throughput *tablestore.ReservedThroughput

	TableStoreConfig *tablestore.TableStoreConfig
	WriterConfig     *writer.Config
}

type Schema struct {
	FirstPk  string
	SecondPk string
}

func (b *StoreOption) prepare(withClient bool) error {
	if !withClient {
		if b.Endpoint == "" || b.Instance == "" || b.AkId == "" || b.AkSecret == "" {
			// require go 1.13
			return fmt.Errorf("%w empty option", ErrMisuse)
		}
	}
	if b.TableName == "" {
		// require go 1.13
		return fmt.Errorf("%w invalid table name", ErrMisuse)
	}

	// fill in default value if empty
	if b.Schema == nil {
		b.Schema = &Schema{FirstPk: DefaultFirstPk, SecondPk: DefaultSecondPk}
	}
	if b.Schema.FirstPk == "" {
		b.Schema.FirstPk = DefaultFirstPk
	}
	if b.Schema.SecondPk == "" {
		b.Schema.SecondPk = DefaultSecondPk
	}
	if b.TTL > 0 && b.TTL < MinTTL {
		b.TTL = MinTTL
	}
	if b.TTL == 0 {
		b.TTL = -1
	}
	if b.Throughput == nil {
		b.Throughput = new(tablestore.ReservedThroughput)
	}
	return nil
}
