package restore

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
	"testing"
)

// TestProcessPreviousBatch tests the processPreviousBatch function.
func TestProcessPreviousBatch(t *testing.T) {
	// Test case 1: No duplicate primary keys
	cnt := 0
	currentBatch := []*tunnel.Record{
		{
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{ColumnName: "id", Value: int64(1)},
				},
			},
		},
		{
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{ColumnName: "id", Value: int64(2)},
				},
			},
		},
	}
	nextBatch := []*tunnel.Record{}
	replayRecords := []*tunnel.Record{}
	recordMap := map[string]bool{}

	cnt, nextBatch, replayRecords = processPreviousBatch(cnt, currentBatch, nextBatch, replayRecords, recordMap)

	if cnt != 2 {
		t.Errorf("Expected cnt to be 2, got %d", cnt)
	}
	if len(nextBatch) != 0 {
		t.Errorf("Expected nextBatch to be empty, got %v", nextBatch)
	}
	if len(replayRecords) != 2 {
		t.Errorf("Expected replayRecords to have 2 records, got %v", replayRecords)
	}

	// Test case 2: Duplicate primary keys
	cnt = 0
	currentBatch = []*tunnel.Record{
		{
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{ColumnName: "id", Value: int64(1)},
				},
			},
		},
		{
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{ColumnName: "id", Value: int64(1)},
				},
			},
		},
		{
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{ColumnName: "id", Value: int64(2)},
				},
			},
		},
	}
	nextBatch = []*tunnel.Record{}
	replayRecords = []*tunnel.Record{}
	recordMap = map[string]bool{}

	cnt, nextBatch, replayRecords = processPreviousBatch(cnt, currentBatch, nextBatch, replayRecords, recordMap)

	if cnt != 2 {
		t.Errorf("Expected cnt to be 2, got %d", cnt)
	}
	if len(nextBatch) != 1 {
		t.Errorf("Expected nextBatch to have 1 record, got %v", nextBatch)
	}
	if len(replayRecords) != 2 {
		t.Errorf("Expected replayRecords to have 2 records, got %v", replayRecords)
	}

	// Test case 3: cnt is not 0
	cnt = 1
	currentBatch = []*tunnel.Record{
		{
			PrimaryKey: &tunnel.PrimaryKey{
				PrimaryKeys: []*tunnel.PrimaryKeyColumn{
					{ColumnName: "id", Value: int64(1)},
				},
			},
		},
	}
	nextBatch = []*tunnel.Record{}
	replayRecords = []*tunnel.Record{}
	recordMap = map[string]bool{}

	cnt, nextBatch, replayRecords = processPreviousBatch(cnt, currentBatch, nextBatch, replayRecords, recordMap)

	if cnt != 1 {
		t.Errorf("Expected cnt to remain 1, got %d", cnt)
	}
	if len(nextBatch) != 0 {
		t.Errorf("Expected nextBatch to be empty, got %v", nextBatch)
	}
	if len(replayRecords) != 0 {
		t.Errorf("Expected replayRecords to be empty, got %v", replayRecords)
	}
}
