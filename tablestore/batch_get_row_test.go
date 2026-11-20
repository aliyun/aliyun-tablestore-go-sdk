package tablestore

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

func newBatchGetPrimaryKey(value string) *PrimaryKey {
	primaryKey := new(PrimaryKey)
	primaryKey.AddPrimaryKeyColumn("pk", value)
	return primaryKey
}

func newBatchGetCriteria(tableName string, maxVersion int) *MultiRowQueryCriteria {
	return &MultiRowQueryCriteria{TableName: tableName, MaxVersion: maxVersion}
}

func newBatchGetRowBytes(primaryKey string, columns ...*AttributeColumn) []byte {
	rowChange := &PutRowChange{PrimaryKey: newBatchGetPrimaryKey(primaryKey)}
	for _, column := range columns {
		rowChange.Columns = append(rowChange.Columns, *column)
	}
	return rowChange.Serialize()
}

func newSuccessfulBatchGetRow(row []byte) *otsprotocol.RowInBatchGetRowResponse {
	return &otsprotocol.RowInBatchGetRowResponse{
		IsOk: proto.Bool(true),
		Consumed: &otsprotocol.ConsumedCapacity{
			CapacityUnit: &otsprotocol.CapacityUnit{
				Read:  proto.Int32(1),
				Write: proto.Int32(0),
			},
		},
		Row: row,
	}
}

func TestBatchGetRowRequestAddMultiRowQueryCriteria(t *testing.T) {
	request := new(BatchGetRowRequest)
	first := newBatchGetCriteria("table", 1)
	other := newBatchGetCriteria("other", 1)
	replacement := newBatchGetCriteria("table", 2)

	// Keep direct append backward compatible, including cleaning up duplicates
	// that may already exist before callers adopt the Add method.
	request.MultiRowQueryCriteria = append(request.MultiRowQueryCriteria, first, other, first)
	request.AddMultiRowQueryCriteria(replacement)
	request.AddMultiRowQueryCriteria(nil)

	if len(request.MultiRowQueryCriteria) != 2 {
		t.Fatalf("expected two unique table criteria, got %d", len(request.MultiRowQueryCriteria))
	}
	if request.MultiRowQueryCriteria[0] != replacement {
		t.Fatal("expected duplicate table criteria to be replaced in place")
	}
	if request.MultiRowQueryCriteria[1] != other {
		t.Fatal("expected the order of other tables to be preserved")
	}
}

func TestBatchGetRowRequestAddMultiRowQueryCriteriaDoesNotMutateAliasedSlice(t *testing.T) {
	first := newBatchGetCriteria("table", 1)
	other := newBatchGetCriteria("other", 1)
	duplicate := newBatchGetCriteria("table", 2)
	replacement := newBatchGetCriteria("table", 3)
	aliased := []*MultiRowQueryCriteria{first, other, duplicate}
	request := &BatchGetRowRequest{MultiRowQueryCriteria: aliased}

	request.AddMultiRowQueryCriteria(replacement)

	if got, want := aliased, []*MultiRowQueryCriteria{first, other, duplicate}; !reflect.DeepEqual(got, want) {
		t.Fatalf("caller's aliased slice was mutated: got %v, want %v", got, want)
	}
	if got, want := request.MultiRowQueryCriteria, []*MultiRowQueryCriteria{replacement, other}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected normalized criteria: got %v, want %v", got, want)
	}
}

func TestBuildBatchGetRowRequestDeduplicatesTables(t *testing.T) {
	first := newBatchGetCriteria("table", 1)
	first.AddRow(newBatchGetPrimaryKey("old"))
	replacement := newBatchGetCriteria("table", 2)
	replacement.AddRow(newBatchGetPrimaryKey("new"))
	request := &BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{first, replacement}}

	pbRequest, plan, err := buildBatchGetRowRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(pbRequest.Tables) != 1 {
		t.Fatalf("expected one table in the protocol request, got %d", len(pbRequest.Tables))
	}
	if pbRequest.Tables[0].GetMaxVersions() != 2 {
		t.Fatalf("expected the last criteria to win, got max versions %d", pbRequest.Tables[0].GetMaxVersions())
	}
	if len(plan.rowOrder) != 1 || plan.criteriaByTable["table"] != replacement {
		t.Fatal("expected response plan to be built from the replacement criteria")
	}
}

func TestBuildBatchGetRowRequestUsesUnionOfPerRowColumns(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("one"), "column_b", "column_a")
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("two"), "column_c", "column_b")
	request := new(BatchGetRowRequest)
	request.AddMultiRowQueryCriteria(criteria)

	pbRequest, _, err := buildBatchGetRowRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"column_b", "column_a", "column_c"}
	if got := pbRequest.Tables[0].GetColumnsToGet(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected columns_to_get: got %v, want %v", got, want)
	}
}

func TestBuildBatchGetRowRequestFallsBackToTableColumns(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.ColumnsToGet = []string{"table_column"}
	criteria.AddRow(newBatchGetPrimaryKey("uses-table-columns"))
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("uses-row-columns"), "row_column")

	pbRequest, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"table_column", "row_column"}
	if got := pbRequest.Tables[0].GetColumnsToGet(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected columns_to_get: got %v, want %v", got, want)
	}
	if got := columnsToGetForRow(criteria, 0); !reflect.DeepEqual(got, []string{"table_column"}) {
		t.Fatalf("expected AddRow to use table columns, got %v", got)
	}
	if got := columnsToGetForRow(criteria, 1); !reflect.DeepEqual(got, []string{"row_column"}) {
		t.Fatalf("expected row columns to take precedence, got %v", got)
	}
}

func TestBuildBatchGetRowRequestReadsAllColumnsWhenOneRowRequestsAll(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("one"), "column_a")
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("two"))

	pbRequest, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if err != nil {
		t.Fatal(err)
	}
	if got := pbRequest.Tables[0].GetColumnsToGet(); len(got) != 0 {
		t.Fatalf("expected an empty projection to read all columns, got %v", got)
	}
}

func TestBuildBatchGetRowRequestRejectsStalePerRowProjections(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("one"), "column_a")
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("two"), "column_b")
	criteria.PrimaryKey[0], criteria.PrimaryKey[1] = criteria.PrimaryKey[1], criteria.PrimaryKey[0]

	_, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if !errors.Is(err, errInvalidInput) {
		t.Fatalf("expected reordered primary keys to be rejected, got %v", err)
	}
	if got, want := err.Error(), "table \"table\" row 0 primary key was replaced"; !strings.Contains(got, want) {
		t.Fatalf("expected a diagnostic projection error, got %q", got)
	}
}

func TestBuildBatchGetRowRequestPrunesProjectionAfterDirectTruncation(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("one"), "column_a")
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("two"), "column_b")
	primaryKeys := criteria.PrimaryKey
	criteria.PrimaryKey = criteria.PrimaryKey[:1]

	pbRequest, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if err != nil {
		t.Fatalf("expected directly truncated criteria to remain valid, got %v", err)
	}
	if got, want := pbRequest.Tables[0].GetColumnsToGet(), []string{"column_a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected projection after truncation: got %v, want %v", got, want)
	}

	criteria.PrimaryKey = primaryKeys
	pbRequest, _, err = buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if err != nil {
		t.Fatalf("expected restored criteria to retain its projection, got %v", err)
	}
	if got, want := pbRequest.Tables[0].GetColumnsToGet(), []string{"column_a", "column_b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected projection after restoring rows: got %v, want %v", got, want)
	}
}

func TestBuildBatchGetRowRequestAllowsCriteriaReuseAfterRowsAreCleared(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("one"), "column_a")
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("two"), "column_b")
	criteria.PrimaryKey = nil
	criteria.AddRow(newBatchGetPrimaryKey("three"))

	pbRequest, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if err != nil {
		t.Fatalf("expected reused criteria to remain valid, got %v", err)
	}
	if got := pbRequest.Tables[0].GetColumnsToGet(); len(got) != 0 {
		t.Fatalf("expected a row added without projection to read all columns, got %v", got)
	}
	if len(criteria.columnsToGetByRow) != 0 {
		t.Fatalf("expected stale projections to be cleared, got %v", criteria.columnsToGetByRow)
	}
}

func TestGenerateBatchGetRowRetryPreservesUnionProjection(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("one"), "column_a")
	criteria.AddRowWithColumnsToGet(newBatchGetPrimaryKey("two"), "column_b")
	pbRequest, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if err != nil {
		t.Fatal(err)
	}
	pbResponse := &otsprotocol.BatchGetRowResponse{Tables: []*otsprotocol.TableInBatchGetRowResponse{
		{
			TableName: proto.String("table"),
			Rows: []*otsprotocol.RowInBatchGetRowResponse{
				{
					IsOk:  proto.Bool(false),
					Error: &otsprotocol.Error{Code: proto.String(SERVER_BUSY), Message: proto.String("retry")},
				},
				newSuccessfulBatchGetRow(newBatchGetRowBytes("two")),
			},
		},
	}}

	body, shouldRetry, err := generateBatchRequest(pbResponse, pbRequest, batchGetRowUri)
	if err != nil {
		t.Fatal(err)
	}
	if !shouldRetry {
		t.Fatal("expected the retryable row error to generate a retry request")
	}
	retryRequest := new(otsprotocol.BatchGetRowRequest)
	if err := proto.Unmarshal(body, retryRequest); err != nil {
		t.Fatal(err)
	}
	if len(retryRequest.Tables) != 1 || len(retryRequest.Tables[0].PrimaryKey) != 1 {
		t.Fatalf("unexpected retry request: %+v", retryRequest)
	}
	if !bytes.Equal(retryRequest.Tables[0].PrimaryKey[0], pbRequest.Tables[0].PrimaryKey[0]) {
		t.Fatal("expected the failed primary key to be retried")
	}
	if got, want := retryRequest.Tables[0].GetColumnsToGet(), []string{"column_a", "column_b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retry projection changed: got %v, want %v", got, want)
	}
}

func TestFillBatchGetRowResponseFiltersColumnsAndPreservesRequestOrder(t *testing.T) {
	firstTable := newBatchGetCriteria("first", 1)
	firstTable.AddRowWithColumnsToGet(newBatchGetPrimaryKey("first-0"), "column_a")
	firstTable.AddRowWithColumnsToGet(newBatchGetPrimaryKey("first-1"), "column_b")
	secondTable := newBatchGetCriteria("second", 1)
	secondTable.AddRowWithColumnsToGet(newBatchGetPrimaryKey("second-0"), "column_c")
	request := &BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{firstTable, secondTable}}
	_, plan, err := buildBatchGetRowRequest(request)
	if err != nil {
		t.Fatal(err)
	}

	allColumns := []*AttributeColumn{
		{ColumnName: "column_a", Value: "a"},
		{ColumnName: "column_b", Value: "b"},
		{ColumnName: "column_c", Value: "c"},
	}
	// Deliberately reverse the table response order. RowResults must follow the request.
	pbResponse := &otsprotocol.BatchGetRowResponse{Tables: []*otsprotocol.TableInBatchGetRowResponse{
		{
			TableName: proto.String("second"),
			Rows:      []*otsprotocol.RowInBatchGetRowResponse{newSuccessfulBatchGetRow(newBatchGetRowBytes("second-0", allColumns...))},
		},
		{
			TableName: proto.String("first"),
			Rows: []*otsprotocol.RowInBatchGetRowResponse{
				newSuccessfulBatchGetRow(newBatchGetRowBytes("first-0", allColumns...)),
				newSuccessfulBatchGetRow(newBatchGetRowBytes("first-1", allColumns...)),
			},
		},
	}}
	response := &BatchGetRowResponse{TableToRowsResult: make(map[string][]RowResult)}
	if err := fillBatchGetRowResponse(response, pbResponse, plan); err != nil {
		t.Fatal(err)
	}

	wantTableNames := []string{"first", "first", "second"}
	wantPrimaryKeys := []string{"first-0", "first-1", "second-0"}
	wantColumns := []string{"column_a", "column_b", "column_c"}
	if len(response.RowResults) != len(wantTableNames) {
		t.Fatalf("unexpected ordered result count: got %d, want %d", len(response.RowResults), len(wantTableNames))
	}
	for index, rowResult := range response.RowResults {
		if rowResult.TableName != wantTableNames[index] {
			t.Errorf("result %d table: got %q, want %q", index, rowResult.TableName, wantTableNames[index])
		}
		if len(rowResult.PrimaryKey.PrimaryKeys) != 1 || rowResult.PrimaryKey.PrimaryKeys[0].Value != wantPrimaryKeys[index] {
			t.Errorf("result %d primary key: got %v, want %q", index, rowResult.PrimaryKey.PrimaryKeys, wantPrimaryKeys[index])
		}
		if len(rowResult.Columns) != 1 || rowResult.Columns[0].ColumnName != wantColumns[index] {
			t.Errorf("result %d columns: got %v, want %q", index, rowResult.Columns, wantColumns[index])
		}
	}
	if !reflect.DeepEqual(response.TableToRowsResult["first"], response.RowResults[:2]) {
		t.Fatal("expected the legacy table map to remain populated with the same per-row projections")
	}
}

func TestFilterBatchGetRowColumnsPreservesServiceColumnOrder(t *testing.T) {
	columns := []*AttributeColumn{
		{ColumnName: "column_b"},
		{ColumnName: "column_a"},
		{ColumnName: "column_c"},
	}
	got := filterBatchGetRowColumns(columns, []string{"column_c", "column_b"})
	want := []*AttributeColumn{columns[0], columns[2]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected filtered columns: got %v, want %v", got, want)
	}
}

func TestFillBatchGetRowResponseRejectsMissingRows(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRow(newBatchGetPrimaryKey("one"))
	criteria.AddRow(newBatchGetPrimaryKey("two"))
	_, plan, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if err != nil {
		t.Fatal(err)
	}
	pbResponse := &otsprotocol.BatchGetRowResponse{Tables: []*otsprotocol.TableInBatchGetRowResponse{
		{
			TableName: proto.String("table"),
			Rows:      []*otsprotocol.RowInBatchGetRowResponse{newSuccessfulBatchGetRow(newBatchGetRowBytes("one"))},
		},
	}}
	response := &BatchGetRowResponse{TableToRowsResult: make(map[string][]RowResult)}
	if err := fillBatchGetRowResponse(response, pbResponse, plan); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("expected a missing row response to be rejected as ErrInvalidResponse, got %v", err)
	}
}

func TestFillBatchGetRowResponsePreservesResponseInfoOnInvalidResponse(t *testing.T) {
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRow(newBatchGetPrimaryKey("one"))
	_, plan, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}})
	if err != nil {
		t.Fatal(err)
	}
	response := &BatchGetRowResponse{
		TableToRowsResult: make(map[string][]RowResult),
		ResponseInfo:      ResponseInfo{RequestId: "request-id"},
	}
	if err := fillBatchGetRowResponse(response, new(otsprotocol.BatchGetRowResponse), plan); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("expected an empty response to be rejected as ErrInvalidResponse, got %v", err)
	}
	if response.RequestId != "request-id" {
		t.Fatalf("response info changed after a decode error: got %q", response.RequestId)
	}
	if len(response.TableToRowsResult) != 0 || len(response.RowResults) != 0 {
		t.Fatalf("invalid response exposed partial row results: tables=%v rows=%v", response.TableToRowsResult, response.RowResults)
	}
}

func TestBuildBatchGetRowRequestRejectsInvalidInput(t *testing.T) {
	if _, _, err := buildBatchGetRowRequest(nil); err == nil {
		t.Fatal("expected a nil request to be rejected")
	}
	if _, _, err := buildBatchGetRowRequest(new(BatchGetRowRequest)); err == nil {
		t.Fatal("expected an empty request to be rejected")
	}
	if _, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{nil}}); err == nil {
		t.Fatal("expected nil criteria to be rejected")
	}
	emptyCriteria := newBatchGetCriteria("table", 1)
	if _, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{emptyCriteria}}); err == nil {
		t.Fatal("expected criteria without primary keys to be rejected")
	}
	criteria := newBatchGetCriteria("table", 1)
	criteria.AddRow(nil)
	if _, _, err := buildBatchGetRowRequest(&BatchGetRowRequest{MultiRowQueryCriteria: []*MultiRowQueryCriteria{criteria}}); err == nil {
		t.Fatal("expected a nil primary key to be rejected")
	}
}

func TestBatchGetRowTestRowEncoding(t *testing.T) {
	rowBytes := newBatchGetRowBytes("primary-key", &AttributeColumn{ColumnName: "column", Value: "value"})
	rows, err := readRowsWithHeader(bytes.NewReader(rowBytes))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0].primaryKey) != 1 || len(rows[0].cells) != 1 {
		t.Fatalf("unexpected test row encoding: %+v", rows)
	}
}
