package sql_test

import (
	"fmt"
	"testing"
	"time"

	tablestore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/testConfig"
	. "gopkg.in/check.v1"
)

// Independent entry point for NonTxnTableSQLSuite.
// Run with: go test ./tablestore/sql/ -run TestNonTxnTableSQL
func TestNonTxnTableSQL(t *testing.T) {
	TestingT(t)
}

const nonTxnTableDMLErrorMsg = "Non-transactional tables currently support atomic batch inserts within a single partition key, and single-row updates or deletes specified by the full primary key without any attribute column conditions."

// =====================================================
// NonTxnTableSQLSuite — independent suite for non-txn table DML tests
// =====================================================

type NonTxnTableSQLSuite struct {
	client tablestore.TableStoreApi
}

var _ = Suite(&NonTxnTableSQLSuite{})

func (s *NonTxnTableSQLSuite) SetUpSuite(c *C) {
	s.client = tablestore.NewClient(testConfig.OtsEndpoint, testConfig.InstanceName, testConfig.OtsAccessId, testConfig.OtsAccessKey)
}

// =====================================================
// Helper methods
// =====================================================

func (s *NonTxnTableSQLSuite) createNonTxnTable(c *C, tableName string, pkNames []string, pkTypes []tablestore.PrimaryKeyType,
	defColNames []string, defColTypes []tablestore.DefinedColumnType) {
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	for i, pkName := range pkNames {
		tableMeta.AddPrimaryKeyColumn(pkName, pkTypes[i])
	}
	for i, colName := range defColNames {
		tableMeta.AddDefinedColumn(colName, defColTypes[i])
	}
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	enableLocalTxn := false
	createReq := &tablestore.CreateTableRequest{
		TableMeta:          tableMeta,
		TableOption:        tableOption,
		ReservedThroughput: &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0},
		EnableLocalTxn:     &enableLocalTxn,
	}
	_, err := s.client.CreateTable(createReq)
	c.Assert(err, IsNil)

	time.Sleep(200 * time.Millisecond)

	// Build SQL CREATE TABLE binding
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", tableName)
	for i, pkName := range pkNames {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` %s", pkName, nonTxnPkTypeToSQL(pkTypes[i]))
	}
	for i, colName := range defColNames {
		sql += fmt.Sprintf(", `%s` %s", colName, nonTxnDefColTypeToSQL(defColTypes[i]))
	}
	sql += ", PRIMARY KEY("
	for i, pkName := range pkNames {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s`", pkName)
	}
	sql += "))"
	_, err = s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: sql})
	c.Assert(err, IsNil)
}

func nonTxnPkTypeToSQL(pkType tablestore.PrimaryKeyType) string {
	switch pkType {
	case tablestore.PrimaryKeyType_INTEGER:
		return "BIGINT"
	case tablestore.PrimaryKeyType_STRING:
		return "VARCHAR(1024)"
	case tablestore.PrimaryKeyType_BINARY:
		return "MEDIUMBLOB"
	default:
		return "BIGINT"
	}
}

func nonTxnDefColTypeToSQL(colType tablestore.DefinedColumnType) string {
	switch colType {
	case tablestore.DefinedColumn_INTEGER:
		return "BIGINT"
	case tablestore.DefinedColumn_DOUBLE:
		return "DOUBLE"
	case tablestore.DefinedColumn_STRING:
		return "MEDIUMTEXT"
	case tablestore.DefinedColumn_BOOLEAN:
		return "BOOL"
	case tablestore.DefinedColumn_BINARY:
		return "MEDIUMBLOB"
	default:
		return "BIGINT"
	}
}

func (s *NonTxnTableSQLSuite) createNonTxnTableWithSinglePK(c *C, tableName string, pkName string, pkType tablestore.PrimaryKeyType,
	defColNames []string, defColTypes []tablestore.DefinedColumnType) {
	s.createNonTxnTable(c, tableName, []string{pkName}, []tablestore.PrimaryKeyType{pkType}, defColNames, defColTypes)
}

func (s *NonTxnTableSQLSuite) createNonTxnTableWithCompositePK(c *C, tableName string, pkNames []string, pkTypes []tablestore.PrimaryKeyType,
	defColNames []string, defColTypes []tablestore.DefinedColumnType) {
	s.createNonTxnTable(c, tableName, pkNames, pkTypes, defColNames, defColTypes)
}

func (s *NonTxnTableSQLSuite) createNonTxnPhysicalTable(c *C, tableName string, pkName string, pkType tablestore.PrimaryKeyType,
	defColNames []string, defColTypes []tablestore.DefinedColumnType) {
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn(pkName, pkType)
	for i, colName := range defColNames {
		tableMeta.AddDefinedColumn(colName, defColTypes[i])
	}
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	enableLocalTxn := false
	createReq := &tablestore.CreateTableRequest{
		TableMeta:          tableMeta,
		TableOption:        tableOption,
		ReservedThroughput: &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0},
		EnableLocalTxn:     &enableLocalTxn,
	}
	_, err := s.client.CreateTable(createReq)
	c.Assert(err, IsNil)
	time.Sleep(200 * time.Millisecond)
}

func (s *NonTxnTableSQLSuite) dropTableQuietly(tableName string) {
	s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: fmt.Sprintf("DROP MAPPING TABLE IF EXISTS %s", tableName)})
	s.client.DeleteTable(&tablestore.DeleteTableRequest{TableName: tableName})
}

func (s *NonTxnTableSQLSuite) execSQL(c *C, sql string) {
	_, err := s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: sql})
	c.Assert(err, IsNil)
}

func (s *NonTxnTableSQLSuite) querySQL(c *C, sql string) *tablestore.SQLQueryResponse {
	resp, err := s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: sql})
	c.Assert(err, IsNil)
	return resp
}

func (s *NonTxnTableSQLSuite) queryRows(c *C, sql string) []tablestore.SQLRow {
	resp := s.querySQL(c, sql)
	var rows []tablestore.SQLRow
	if resp.ResultSet != nil {
		for resp.ResultSet.HasNext() {
			rows = append(rows, resp.ResultSet.Next())
		}
	}
	return rows
}

func (s *NonTxnTableSQLSuite) assertQueryError(c *C, sql string, expectedErrorCode string, expectedErrorMsg string) {
	_, err := s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: sql})
	c.Assert(err, NotNil)
	otsErr, ok := err.(*tablestore.OtsError)
	if ok {
		c.Assert(otsErr.Code, Equals, expectedErrorCode)
		c.Check(otsErr.Message, Matches, ".*"+expectedErrorMsg+".*")
	}
}

func (s *NonTxnTableSQLSuite) assertQueryFails(c *C, sql string) {
	_, err := s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: sql})
	c.Assert(err, NotNil)
}

// =====================================================
// Point Delete Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestPointDeleteBasic(c *C) {
	tableName := "nontxn_sql_test_point_delete_basic"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c", "d"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER, tablestore.DefinedColumn_STRING})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 'a')", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 2, 'b')", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (3, 3, 'c')", tableName))

	deleteResp := s.querySQL(c, fmt.Sprintf("DELETE FROM %s WHERE id = 1", tableName))
	c.Assert(deleteResp.StmtType, Equals, tablestore.SQL_DELETE)

	selectResp := s.querySQL(c, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	c.Assert(selectResp.StmtType, Equals, tablestore.SQL_SELECT)
	var rows []tablestore.SQLRow
	if selectResp.ResultSet != nil {
		for selectResp.ResultSet.HasNext() {
			rows = append(rows, selectResp.ResultSet.Next())
		}
	}
	c.Assert(len(rows), Equals, 2)
	id0, _ := rows[0].GetInt64ByName("id")
	id1, _ := rows[1].GetInt64ByName("id")
	c.Assert(id0, Equals, int64(2))
	c.Assert(id1, Equals, int64(3))
}

func (s *NonTxnTableSQLSuite) TestPointDeleteCompositePK(c *C) {
	tableName := "nontxn_sql_test_point_delete_composite_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 2, 200)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 1, 300)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 2, 400)", tableName))

	s.execSQL(c, fmt.Sprintf("DELETE FROM %s WHERE pk1 = 1 AND pk2 = 1", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY pk1, pk2", tableName))
	c.Assert(len(rows), Equals, 3)
	pk1_0, _ := rows[0].GetInt64ByName("pk1")
	pk2_0, _ := rows[0].GetInt64ByName("pk2")
	c.Assert(pk1_0, Equals, int64(1))
	c.Assert(pk2_0, Equals, int64(2))
}

func (s *NonTxnTableSQLSuite) TestPointDeleteNonExistentRow(c *C) {
	tableName := "nontxn_sql_test_point_delete_nonexist"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	// Delete non-existent row — should succeed without error
	s.execSQL(c, fmt.Sprintf("DELETE FROM %s WHERE id = 999", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	val, _ := rows[0].GetInt64ByName("c")
	c.Assert(val, Equals, int64(100))
}

func (s *NonTxnTableSQLSuite) TestPointDeleteWithNullValues(c *C) {
	tableName := "nontxn_sql_test_point_delete_null"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c", "d"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER, tablestore.DefinedColumn_STRING})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 'a')", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, NULL, NULL)", tableName))

	s.execSQL(c, fmt.Sprintf("DELETE FROM %s WHERE id = 2", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	id, _ := rows[0].GetInt64ByName("id")
	c.Assert(id, Equals, int64(1))
}

func (s *NonTxnTableSQLSuite) TestPointDeleteWithVarcharPK(c *C) {
	tableName := "nontxn_sql_test_point_delete_varchar_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_STRING,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES ('abc', 1)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES ('def', 2)", tableName))

	s.execSQL(c, fmt.Sprintf("DELETE FROM %s WHERE id = 'abc'", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	id, _ := rows[0].GetStringByName("id")
	c.Assert(id, Equals, "def")
}

func (s *NonTxnTableSQLSuite) TestPointDeleteWithBigIntPK(c *C) {
	tableName := "nontxn_sql_test_point_delete_bigint_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// Max long value should fail
	s.assertQueryError(c, fmt.Sprintf("INSERT INTO %s VALUES (9223372036854775807, 1)", tableName),
		"OTSParameterInvalid", "The input parameter is invalid.")

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (9223372036854775806, 1)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 2)", tableName))

	s.execSQL(c, fmt.Sprintf("DELETE FROM %s WHERE id = 9223372036854775806", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	id, _ := rows[0].GetInt64ByName("id")
	c.Assert(id, Equals, int64(1))
}

func (s *NonTxnTableSQLSuite) TestPointDeleteWithLimit(c *C) {
	tableName := "nontxn_sql_test_point_delete_limit"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 200)", tableName))

	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE id = 1 LIMIT 1", tableName),
		"OTSUnsupportOperation", "limit in delete statement is not supported")

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	c.Assert(len(rows), Equals, 2)
}

func (s *NonTxnTableSQLSuite) TestPointDeleteDifferentPKOrder(c *C) {
	tableName := "nontxn_sql_test_point_delete_pk_order"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 2, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 1, 200)", tableName))

	// WHERE clause PK order reversed from definition
	s.execSQL(c, fmt.Sprintf("DELETE FROM %s WHERE pk2 = 2 AND pk1 = 1", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	pk1, _ := rows[0].GetInt64ByName("pk1")
	pk2, _ := rows[0].GetInt64ByName("pk2")
	c.Assert(pk1, Equals, int64(2))
	c.Assert(pk2, Equals, int64(1))
}

func (s *NonTxnTableSQLSuite) TestPointDeleteIllegal(c *C) {
	tableName := "nontxn_sql_test_point_delete_illegal"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 200)", tableName))

	// Missing WHERE clause
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Non-PK column in WHERE
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE c = 100", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Range query (greater than)
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE id > 1", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Range query (less than or equal)
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE id <= 2", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// IN clause
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE id IN (1, 2)", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// OR condition
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE id = 1 OR id = 2", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// BETWEEN condition
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE id BETWEEN 1 AND 2", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Not equal condition
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE id != 1", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)
}

func (s *NonTxnTableSQLSuite) TestPointDeleteIllegalCompositePK(c *C) {
	tableName := "nontxn_sql_test_point_delete_illegal_cpk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 2, 200)", tableName))

	// Incomplete composite PK
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE pk1 = 1", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Full PK + attribute column condition
	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE pk1 = 1 AND pk2 = 1 AND c > 50", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)
}

// =====================================================
// Point Insert Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestPointInsertBasic(c *C) {
	tableName := "nontxn_sql_test_point_insert_basic"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c", "d"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER, tablestore.DefinedColumn_STRING})

	insertResp1 := s.querySQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 'a')", tableName))
	c.Assert(insertResp1.StmtType, Equals, tablestore.SQL_INSERT)
	insertResp2 := s.querySQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 2, 'b')", tableName))
	c.Assert(insertResp2.StmtType, Equals, tablestore.SQL_INSERT)

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	c.Assert(len(rows), Equals, 2)
	id0, _ := rows[0].GetInt64ByName("id")
	c0, _ := rows[0].GetInt64ByName("c")
	d0, _ := rows[0].GetStringByName("d")
	c.Assert(id0, Equals, int64(1))
	c.Assert(c0, Equals, int64(1))
	c.Assert(d0, Equals, "a")
	id1, _ := rows[1].GetInt64ByName("id")
	c1, _ := rows[1].GetInt64ByName("c")
	d1, _ := rows[1].GetStringByName("d")
	c.Assert(id1, Equals, int64(2))
	c.Assert(c1, Equals, int64(2))
	c.Assert(d1, Equals, "b")
}

func (s *NonTxnTableSQLSuite) TestPointInsertBatchSamePartitionKey(c *C) {
	tableName := "nontxn_sql_test_point_insert_batch"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	resp := s.querySQL(c, fmt.Sprintf(
		"INSERT INTO %s VALUES (1, 1, 100), (1, 2, 200), (1, 3, 300)", tableName))
	c.Assert(resp.StmtType, Equals, tablestore.SQL_INSERT)
	c.Assert(resp.AffectedRows, Equals, int64(3))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY pk1, pk2", tableName))
	c.Assert(len(rows), Equals, 3)
	v0, _ := rows[0].GetInt64ByName("value")
	v1, _ := rows[1].GetInt64ByName("value")
	v2, _ := rows[2].GetInt64ByName("value")
	c.Assert(v0, Equals, int64(100))
	c.Assert(v1, Equals, int64(200))
	c.Assert(v2, Equals, int64(300))
}

func (s *NonTxnTableSQLSuite) TestPointInsertWithNull(c *C) {
	tableName := "nontxn_sql_test_point_insert_null"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c", "d"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER, tablestore.DefinedColumn_STRING})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, NULL, NULL)", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	id, _ := rows[0].GetInt64ByName("id")
	c.Assert(id, Equals, int64(1))
	isNullC, _ := rows[0].IsNullByName("c")
	isNullD, _ := rows[0].IsNullByName("d")
	c.Assert(isNullC, Equals, true)
	c.Assert(isNullD, Equals, true)
}

func (s *NonTxnTableSQLSuite) TestPointInsertWithVarcharPK(c *C) {
	tableName := "nontxn_sql_test_point_insert_varchar_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_STRING,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES ('abc', 1)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES ('def', 2)", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	c.Assert(len(rows), Equals, 2)
	id0, _ := rows[0].GetStringByName("id")
	id1, _ := rows[1].GetStringByName("id")
	c.Assert(id0, Equals, "abc")
	c.Assert(id1, Equals, "def")
}

func (s *NonTxnTableSQLSuite) TestPointInsertDuplicateKey(c *C) {
	tableName := "nontxn_sql_test_point_insert_dup_key"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	// Insert with same PK should fail
	s.assertQueryError(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 200)", tableName),
		"OTSParameterInvalid", "Duplicate entry for key 'PRIMARY'")

	// Verify original data unchanged
	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	val, _ := rows[0].GetInt64ByName("c")
	c.Assert(val, Equals, int64(100))
}

func (s *NonTxnTableSQLSuite) TestPointInsertBatchDifferentPartitionKey(c *C) {
	tableName := "nontxn_sql_test_point_insert_diff_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// Batch insert with different partition keys should fail
	s.assertQueryError(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 100), (2, 1, 200)", tableName),
		"OTSUnsupportOperation", "`insert into` on different partition keys is not supported")
}

func (s *NonTxnTableSQLSuite) TestPointInsertAffectedRows(c *C) {
	tableName := "nontxn_sql_test_point_insert_affected"
	cpkTableName := "nontxn_sql_test_point_insert_affected_cpk"
	s.dropTableQuietly(tableName)
	s.dropTableQuietly(cpkTableName)
	defer s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(cpkTableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// Single row insert
	resp := s.querySQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))
	c.Assert(resp.AffectedRows, Equals, int64(1))

	// Multi-row insert with same partition key
	s.createNonTxnTableWithCompositePK(c, cpkTableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})
	resp = s.querySQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 100), (1, 2, 200), (1, 3, 300)", cpkTableName))
	c.Assert(resp.AffectedRows, Equals, int64(3))
}

func (s *NonTxnTableSQLSuite) TestPointInsertBatchRowLimit(c *C) {
	tableName := "nontxn_sql_test_point_insert_row_limit"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// Build a batch insert with exactly 200 rows — should succeed
	sql200 := fmt.Sprintf("INSERT INTO %s VALUES ", tableName)
	for i := 1; i <= 200; i++ {
		if i > 1 {
			sql200 += ", "
		}
		sql200 += fmt.Sprintf("(1, %d, %d)", i, i*10)
	}
	resp := s.querySQL(c, sql200)
	c.Assert(resp.AffectedRows, Equals, int64(200))

	// Verify 200 rows inserted
	rows := s.queryRows(c, fmt.Sprintf("SELECT COUNT(*) as cnt FROM %s WHERE pk1 = 1", tableName))
	cnt, _ := rows[0].GetInt64ByName("cnt")
	c.Assert(cnt, Equals, int64(200))

	// Build a batch insert with 201 rows — should fail
	sql201 := fmt.Sprintf("INSERT INTO %s VALUES ", tableName)
	for i := 201; i <= 401; i++ {
		if i > 201 {
			sql201 += ", "
		}
		sql201 += fmt.Sprintf("(2, %d, %d)", i, i*10)
	}
	_, err := s.client.SQLQuery(&tablestore.SQLQueryRequest{Query: sql201})
	c.Assert(err, NotNil)
}

// =====================================================
// Point Update Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestPointUpdateBasic(c *C) {
	tableName := "nontxn_sql_test_point_update_basic"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c", "d"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER, tablestore.DefinedColumn_STRING})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 'a')", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 2, 'b')", tableName))

	updateResp := s.querySQL(c, fmt.Sprintf("UPDATE %s SET c = 10 WHERE id = 1", tableName))
	c.Assert(updateResp.StmtType, Equals, tablestore.SQL_UPDATE)

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	c.Assert(len(rows), Equals, 2)
	c0, _ := rows[0].GetInt64ByName("c")
	d0, _ := rows[0].GetStringByName("d")
	c1, _ := rows[1].GetInt64ByName("c")
	c.Assert(c0, Equals, int64(10))
	c.Assert(d0, Equals, "a")
	c.Assert(c1, Equals, int64(2))
}

func (s *NonTxnTableSQLSuite) TestPointUpdateSelfIncrement(c *C) {
	tableName := "nontxn_sql_test_point_update_self_incr"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"counter"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	// counter + 1
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET counter = counter + 1 WHERE id = 1", tableName))
	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	val, _ := rows[0].GetInt64ByName("counter")
	c.Assert(val, Equals, int64(101))

	// counter + 10
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET counter = counter + 10 WHERE id = 1", tableName))
	rows = s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	val, _ = rows[0].GetInt64ByName("counter")
	c.Assert(val, Equals, int64(111))

	// counter - 5
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET counter = counter - 5 WHERE id = 1", tableName))
	rows = s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	val, _ = rows[0].GetInt64ByName("counter")
	c.Assert(val, Equals, int64(106))
}

func (s *NonTxnTableSQLSuite) TestPointUpdateWithMultipleColumns(c *C) {
	tableName := "nontxn_sql_test_point_update_multi_col"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c1", "c2", "c3"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER, tablestore.DefinedColumn_INTEGER, tablestore.DefinedColumn_STRING})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 1, 'a')", tableName))

	s.execSQL(c, fmt.Sprintf("UPDATE %s SET c1 = 10, c2 = 20, c3 = 'updated' WHERE id = 1", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	c1, _ := rows[0].GetInt64ByName("c1")
	c2, _ := rows[0].GetInt64ByName("c2")
	c3, _ := rows[0].GetStringByName("c3")
	c.Assert(c1, Equals, int64(10))
	c.Assert(c2, Equals, int64(20))
	c.Assert(c3, Equals, "updated")
}

func (s *NonTxnTableSQLSuite) TestPointUpdateWithNull(c *C) {
	tableName := "nontxn_sql_test_point_update_null"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, NULL)", tableName))

	// Update NULL to value
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET c = 10 WHERE id = 2", tableName))
	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s WHERE id = 2", tableName))
	val, _ := rows[0].GetInt64ByName("c")
	c.Assert(val, Equals, int64(10))

	// Update value to NULL
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET c = NULL WHERE id = 1", tableName))
	rows = s.queryRows(c, fmt.Sprintf("SELECT * FROM %s WHERE id = 1", tableName))
	isNull, _ := rows[0].IsNullByName("c")
	c.Assert(isNull, Equals, true)
}

func (s *NonTxnTableSQLSuite) TestPointUpdateCompositePK(c *C) {
	tableName := "nontxn_sql_test_point_update_composite_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 2, 200)", tableName))

	s.execSQL(c, fmt.Sprintf("UPDATE %s SET value = 999 WHERE pk1 = 1 AND pk2 = 1", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY pk1, pk2", tableName))
	c.Assert(len(rows), Equals, 2)
	v0, _ := rows[0].GetInt64ByName("value")
	v1, _ := rows[1].GetInt64ByName("value")
	c.Assert(v0, Equals, int64(999))
	c.Assert(v1, Equals, int64(200))
}

func (s *NonTxnTableSQLSuite) TestPointUpdateNonExistentRow(c *C) {
	tableName := "nontxn_sql_test_point_update_nonexist"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	// Update non-existent row — should succeed without error
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id = 999", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	val, _ := rows[0].GetInt64ByName("c")
	c.Assert(val, Equals, int64(100))
}

func (s *NonTxnTableSQLSuite) TestPointUpdateWithVarcharPK(c *C) {
	tableName := "nontxn_sql_test_point_update_varchar_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_STRING,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES ('abc', 1)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES ('def', 2)", tableName))

	s.execSQL(c, fmt.Sprintf("UPDATE %s SET c = 100 WHERE id = 'abc'", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	c.Assert(len(rows), Equals, 2)
	c0, _ := rows[0].GetInt64ByName("c")
	c1, _ := rows[1].GetInt64ByName("c")
	c.Assert(c0, Equals, int64(100))
	c.Assert(c1, Equals, int64(2))
}

func (s *NonTxnTableSQLSuite) TestPointUpdateWithLimit(c *C) {
	tableName := "nontxn_sql_test_point_update_limit"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 200)", tableName))

	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id = 1 LIMIT 1", tableName),
		"OTSUnsupportOperation", "limit in update statement is not supported")

	// Verify data unchanged
	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY id", tableName))
	c.Assert(len(rows), Equals, 2)
	c0, _ := rows[0].GetInt64ByName("c")
	c1, _ := rows[1].GetInt64ByName("c")
	c.Assert(c0, Equals, int64(100))
	c.Assert(c1, Equals, int64(200))
}

func (s *NonTxnTableSQLSuite) TestPointUpdateDifferentPKOrder(c *C) {
	tableName := "nontxn_sql_test_point_update_pk_order"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 2, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 1, 200)", tableName))

	// WHERE clause PK order reversed from definition
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET value = 999 WHERE pk2 = 2 AND pk1 = 1", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s ORDER BY pk1, pk2", tableName))
	c.Assert(len(rows), Equals, 2)
	v0, _ := rows[0].GetInt64ByName("value")
	v1, _ := rows[1].GetInt64ByName("value")
	c.Assert(v0, Equals, int64(999))
	c.Assert(v1, Equals, int64(200))
}

func (s *NonTxnTableSQLSuite) TestPointUpdateIllegal(c *C) {
	tableName := "nontxn_sql_test_point_update_illegal"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (2, 200)", tableName))

	// Missing WHERE clause
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Non-PK column in WHERE
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE c = 100", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Range query (greater than)
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id > 1", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Range query (less than or equal)
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id <= 2", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// IN clause
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id IN (1, 2)", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// OR condition
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id = 1 OR id = 2", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// BETWEEN condition
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id BETWEEN 1 AND 2", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Not equal condition
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id != 1", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)
}

func (s *NonTxnTableSQLSuite) TestPointUpdateIllegalCompositePK(c *C) {
	tableName := "nontxn_sql_test_point_update_illegal_cpk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 100)", tableName))
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 2, 200)", tableName))

	// Incomplete composite PK
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE pk1 = 1", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)

	// Full PK + attribute column condition
	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE pk1 = 1 AND pk2 = 1 AND c > 50", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)
}

// =====================================================
// Self Increment Error Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestPointUpdateSelfIncrementOnStringColumn(c *C) {
	tableName := "nontxn_sql_test_update_incr_str"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"str_col"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_STRING})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 'hello')", tableName))

	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET str_col = str_col + 1 WHERE id = 1", tableName),
		"OTSUnsupportOperation", "point update only supports constant and self-increment assignments")
}

func (s *NonTxnTableSQLSuite) TestPointUpdateSelfIncrementOnDoubleColumn(c *C) {
	tableName := "nontxn_sql_test_update_incr_double"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"double_col"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_DOUBLE})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1.5)", tableName))

	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET double_col = double_col + 1 WHERE id = 1", tableName),
		"OTSUnsupportOperation", "point update only supports integer type self increment")
}

func (s *NonTxnTableSQLSuite) TestPointUpdateSelfIncrementOnBooleanColumn(c *C) {
	tableName := "nontxn_sql_test_update_incr_bool"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"bool_col"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_BOOLEAN})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, true)", tableName))

	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET bool_col = bool_col + 1 WHERE id = 1", tableName),
		"OTSUnsupportOperation", "point update only supports integer type self increment")
}

// =====================================================
// INSERT IGNORE Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestInsertIgnoreNotSupportedOnNonTxnTable(c *C) {
	tableName := "nontxn_sql_test_insert_ignore_basic"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.assertQueryError(c, fmt.Sprintf("INSERT IGNORE INTO %s VALUES (1, 100)", tableName),
		"OTSParameterInvalid", "Try to call method using explicit transaction on explicit-transaction-disabled table.")
}

func (s *NonTxnTableSQLSuite) TestInsertIgnoreBatchDifferentPartitionKey(c *C) {
	tableName := "nontxn_sql_test_insert_ignore_diff_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.assertQueryError(c, fmt.Sprintf("INSERT IGNORE INTO %s VALUES (1, 1, 100), (2, 1, 200)", tableName),
		"OTSUnsupportOperation", "`insert ignore` on different partition keys is not supported")
}

// =====================================================
// ON DUPLICATE KEY UPDATE Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestOnDuplicateKeyUpdateNotSupportedOnNonTxnTable(c *C) {
	tableName := "nontxn_sql_test_on_dup_insert"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.assertQueryError(c,
		fmt.Sprintf("INSERT INTO %s VALUES (1, 100) ON DUPLICATE KEY UPDATE c = VALUES(c)", tableName),
		"OTSParameterInvalid", "Try to call method using explicit transaction on explicit-transaction-disabled table.")
}

func (s *NonTxnTableSQLSuite) TestOnDuplicateKeyUpdateDifferentPartitionKey(c *C) {
	tableName := "nontxn_sql_test_on_dup_diff_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithCompositePK(c, tableName,
		[]string{"pk1", "pk2"}, []tablestore.PrimaryKeyType{tablestore.PrimaryKeyType_INTEGER, tablestore.PrimaryKeyType_INTEGER},
		[]string{"value"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.assertQueryError(c,
		fmt.Sprintf("INSERT INTO %s VALUES (1, 1, 100), (2, 1, 200) ON DUPLICATE KEY UPDATE value = VALUES(value)", tableName),
		"OTSUnsupportOperation", "`insert ... on duplicate key update` on different partition keys is not supported")
}

// =====================================================
// Affected Rows Tests (UPDATE / DELETE)
// =====================================================

func (s *NonTxnTableSQLSuite) TestPointUpdateAffectedRows(c *C) {
	tableName := "nontxn_sql_test_update_affected"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	resp := s.querySQL(c, fmt.Sprintf("UPDATE %s SET c = 200 WHERE id = 1", tableName))
	c.Assert(resp.AffectedRows, Equals, int64(1))
}

func (s *NonTxnTableSQLSuite) TestPointDeleteAffectedRows(c *C) {
	tableName := "nontxn_sql_test_delete_affected"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	resp := s.querySQL(c, fmt.Sprintf("DELETE FROM %s WHERE id = 1", tableName))
	c.Assert(resp.AffectedRows, Equals, int64(1))
}

// =====================================================
// Illegal Syntax Rejection Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestDeleteWithLikeCondition(c *C) {
	tableName := "nontxn_sql_test_delete_like"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_STRING,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES ('abc', 1)", tableName))

	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE id LIKE 'a%%'", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)
}

func (s *NonTxnTableSQLSuite) TestUpdateWithLikeCondition(c *C) {
	tableName := "nontxn_sql_test_update_like"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_STRING,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES ('abc', 1)", tableName))

	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id LIKE 'a%%'", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)
}

func (s *NonTxnTableSQLSuite) TestDeleteWithIsNullCondition(c *C) {
	tableName := "nontxn_sql_test_delete_isnull"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, NULL)", tableName))

	s.assertQueryError(c, fmt.Sprintf("DELETE FROM %s WHERE c IS NULL", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)
}

func (s *NonTxnTableSQLSuite) TestUpdateWithIsNullCondition(c *C) {
	tableName := "nontxn_sql_test_update_isnull"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, NULL)", tableName))

	s.assertQueryError(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE c IS NULL", tableName),
		"OTSParameterInvalid", nonTxnTableDMLErrorMsg)
}

func (s *NonTxnTableSQLSuite) TestDeleteWithOrderBy(c *C) {
	tableName := "nontxn_sql_test_delete_orderby"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	s.assertQueryFails(c, fmt.Sprintf("DELETE FROM %s WHERE id = 1 ORDER BY id", tableName))
}

func (s *NonTxnTableSQLSuite) TestUpdateWithOrderBy(c *C) {
	tableName := "nontxn_sql_test_update_orderby"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	s.assertQueryFails(c, fmt.Sprintf("UPDATE %s SET c = 999 WHERE id = 1 ORDER BY id", tableName))
}

func (s *NonTxnTableSQLSuite) TestInsertSetSyntax(c *C) {
	tableName := "nontxn_sql_test_insert_set"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.assertQueryFails(c, fmt.Sprintf("INSERT INTO %s SET id = 1, c = 100", tableName))
}

func (s *NonTxnTableSQLSuite) TestInsertSelect(c *C) {
	tableName := "nontxn_sql_test_insert_select"
	sourceTable := "nontxn_sql_test_insert_select_src"
	s.dropTableQuietly(tableName)
	s.dropTableQuietly(sourceTable)
	defer s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(sourceTable)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})
	s.createNonTxnTableWithSinglePK(c, sourceTable, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", sourceTable))

	s.assertQueryFails(c, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", tableName, sourceTable))
}

func (s *NonTxnTableSQLSuite) TestUpdatePrimaryKeyColumn(c *C) {
	tableName := "nontxn_sql_test_update_pk_col"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))

	// Updating PK column should be rejected
	s.assertQueryFails(c, fmt.Sprintf("UPDATE %s SET id = 2 WHERE id = 1", tableName))

	// Verify data unchanged
	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	id, _ := rows[0].GetInt64ByName("id")
	c.Assert(id, Equals, int64(1))
}

// =====================================================
// Data Type Coverage Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestBinaryPrimaryKeyNotSupported(c *C) {
	tableName := "nontxn_sql_test_binary_pk"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	// BINARY (MEDIUMBLOB) is not supported as a primary key type in SQL binding
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn("id", tablestore.PrimaryKeyType_BINARY)
	tableMeta.AddDefinedColumn("c", tablestore.DefinedColumn_INTEGER)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 1
	enableLocalTxn := false
	createReq := &tablestore.CreateTableRequest{
		TableMeta:          tableMeta,
		TableOption:        tableOption,
		ReservedThroughput: &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0},
		EnableLocalTxn:     &enableLocalTxn,
	}
	_, _ = s.client.CreateTable(createReq)
	time.Sleep(200 * time.Millisecond)

	_, err := s.client.SQLQuery(&tablestore.SQLQueryRequest{
		Query: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`id` MEDIUMBLOB, `c` BIGINT, PRIMARY KEY(`id`))", tableName),
	})
	c.Assert(err, NotNil)
}

func (s *NonTxnTableSQLSuite) TestDoubleColumnConstantUpdate(c *C) {
	tableName := "nontxn_sql_test_double_col_update"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"double_col"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_DOUBLE})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 1.5)", tableName))
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET double_col = 3.14 WHERE id = 1", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	val, _ := rows[0].GetFloat64ByName("double_col")
	c.Assert(val > 3.13 && val < 3.15, Equals, true)
}

func (s *NonTxnTableSQLSuite) TestBooleanColumnConstantUpdate(c *C) {
	tableName := "nontxn_sql_test_bool_col_update"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"bool_col"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_BOOLEAN})

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, true)", tableName))
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET bool_col = false WHERE id = 1", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
	val, _ := rows[0].GetBoolByName("bool_col")
	c.Assert(val, Equals, false)
}

func (s *NonTxnTableSQLSuite) TestBlobColumnOperations(c *C) {
	tableName := "nontxn_sql_test_blob_col"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnTableWithSinglePK(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"blob_col"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_BINARY})

	// INSERT with blob value
	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, X'DEADBEEF')", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)

	// UPDATE blob column
	s.execSQL(c, fmt.Sprintf("UPDATE %s SET blob_col = X'CAFEBABE' WHERE id = 1", tableName))

	rows = s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)

	// DELETE row with blob
	s.execSQL(c, fmt.Sprintf("DELETE FROM %s WHERE id = 1", tableName))
	rows = s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 0)
}

// =====================================================
// CREATE TABLE Column Option & Constraint Tests
// =====================================================

func (s *NonTxnTableSQLSuite) TestCreateTableWithNotNull(c *C) {
	tableName := "nontxn_sql_test_create_notnull"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnPhysicalTable(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// NOT NULL column option should be accepted
	s.execSQL(c, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT NOT NULL, PRIMARY KEY(`id`))", tableName))

	s.execSQL(c, fmt.Sprintf("INSERT INTO %s VALUES (1, 100)", tableName))
	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 1)
}

func (s *NonTxnTableSQLSuite) TestCreateTableWithDefaultValue(c *C) {
	tableName := "nontxn_sql_test_create_default"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnPhysicalTable(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// DEFAULT VALUE column option should be accepted
	s.execSQL(c, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT DEFAULT 0, PRIMARY KEY(`id`))", tableName))

	rows := s.queryRows(c, fmt.Sprintf("SELECT * FROM %s", tableName))
	c.Assert(len(rows), Equals, 0)
}

func (s *NonTxnTableSQLSuite) TestCreateTableWithAutoIncrementRejected(c *C) {
	tableName := "nontxn_sql_test_create_autoinc"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnPhysicalTable(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// AUTO_INCREMENT should be rejected
	s.assertQueryFails(c,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT AUTO_INCREMENT, `c` BIGINT, PRIMARY KEY(`id`))", tableName))
}

func (s *NonTxnTableSQLSuite) TestCreateTableWithUniqueKeyRejected(c *C) {
	tableName := "nontxn_sql_test_create_unique"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnPhysicalTable(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// UNIQUE constraint should be rejected
	s.assertQueryFails(c,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT, PRIMARY KEY(`id`), UNIQUE KEY(`c`))", tableName))
}

func (s *NonTxnTableSQLSuite) TestCreateTableWithCheckRejected(c *C) {
	tableName := "nontxn_sql_test_create_check"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnPhysicalTable(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// CHECK constraint should be rejected
	s.assertQueryFails(c,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT CHECK(`c` > 0), PRIMARY KEY(`id`))", tableName))
}

func (s *NonTxnTableSQLSuite) TestCreateTableWithCommentRejected(c *C) {
	tableName := "nontxn_sql_test_create_comment"
	s.dropTableQuietly(tableName)
	defer s.dropTableQuietly(tableName)

	s.createNonTxnPhysicalTable(c, tableName, "id", tablestore.PrimaryKeyType_INTEGER,
		[]string{"c"}, []tablestore.DefinedColumnType{tablestore.DefinedColumn_INTEGER})

	// COMMENT column option should be rejected
	s.assertQueryFails(c,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT COMMENT 'test', PRIMARY KEY(`id`))", tableName))
}
