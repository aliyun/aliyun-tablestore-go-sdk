package tablestore

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
)

func TestSQLStatementType_String(t *testing.T) {
	tests := []struct {
		name     string
		input    *SQLStatementType
		expected string
	}{
		{"nil", nil, "UNKNOWN"},
		{"SELECT", sqlStmtTypePtr(SQL_SELECT), "SQL_SELECT"},
		{"CREATE_TABLE", sqlStmtTypePtr(SQL_CREATE_TABLE), "SQL_CREATE_TABLE"},
		{"SHOW_TABLE", sqlStmtTypePtr(SQL_SHOW_TABLE), "SQL_SHOW_TABLE"},
		{"DESCRIBE_TABLE", sqlStmtTypePtr(SQL_DESCRIBE_TABLE), "SQL_DESCRIBE_TABLE"},
		{"DROP_TABLE", sqlStmtTypePtr(SQL_DROP_TABLE), "SQL_DROP_TABLE"},
		{"ALTER_TABLE", sqlStmtTypePtr(SQL_ALTER_TABLE), "SQL_ALTER_TABLE"},
		{"INSERT", sqlStmtTypePtr(SQL_INSERT), "SQL_INSERT"},
		{"UPDATE", sqlStmtTypePtr(SQL_UPDATE), "SQL_UPDATE"},
		{"DELETE", sqlStmtTypePtr(SQL_DELETE), "SQL_DELETE"},
		{"unknown", sqlStmtTypePtr(SQLStatementType(999)), "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.input.String())
		})
	}
}

func TestFormatSQLStmtTypeFromPB(t *testing.T) {
	tests := []struct {
		name     string
		input    otsprotocol.SQLStatementType
		expected SQLStatementType
	}{
		{"SELECT", otsprotocol.SQLStatementType_SQL_SELECT, SQL_SELECT},
		{"CREATE_TABLE", otsprotocol.SQLStatementType_SQL_CREATE_TABLE, SQL_CREATE_TABLE},
		{"SHOW_TABLE", otsprotocol.SQLStatementType_SQL_SHOW_TABLE, SQL_SHOW_TABLE},
		{"DESCRIBE_TABLE", otsprotocol.SQLStatementType_SQL_DESCRIBE_TABLE, SQL_DESCRIBE_TABLE},
		{"DROP_TABLE", otsprotocol.SQLStatementType_SQL_DROP_TABLE, SQL_DROP_TABLE},
		{"ALTER_TABLE", otsprotocol.SQLStatementType_SQL_ALTER_TABLE, SQL_ALTER_TABLE},
		{"INSERT", otsprotocol.SQLStatementType_SQL_INSERT, SQL_INSERT},
		{"UPDATE", otsprotocol.SQLStatementType_SQL_UPDATE, SQL_UPDATE},
		{"DELETE", otsprotocol.SQLStatementType_SQL_DELETE, SQL_DELETE},
		{"unknown", otsprotocol.SQLStatementType(100), SQLStatementType(-1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, formatSQLStmtTypeFromPB(tt.input))
		})
	}
}

func sqlStmtTypePtr(v SQLStatementType) *SQLStatementType {
	return &v
}
