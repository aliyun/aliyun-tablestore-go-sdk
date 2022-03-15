package sample

import "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

func SQLQuerySample(client *tablestore.TableStoreClient) {
	SQLShowTablesSample(client)
	SQLDropMappingTableSample(client)
	SQLCreateTableSample(client)
	SQLDescribeTableSample(client)
	SQLSelectSample(client)
}

func SQLShowTablesSample(client *tablestore.TableStoreClient) {
	println("BEGIN SQLShowTablesSample")
	request := new(tablestore.SQLQueryRequest)
	request.Query = "show tables"
	response, err := client.SQLQuery(request)
	if err != nil {
		println("[Info]: show tables failed with error: ", err.Error())
		return
	}
	resultSet := response.ResultSet
	for resultSet.HasNext() {
		row := resultSet.Next()
		// tableName at 0 colIdx
		tableName, err := row.GetString(0)
		if err != nil {
			println("[Info]: parse table name failed with error: ", err.Error())
			continue
		}
		println("tableName: ", tableName)
	}
	println("END SQLShowTablesSample")
}

func SQLDropMappingTableSample(client *tablestore.TableStoreClient) {
	println("BEGIN SQLDropMappingTableSample")
	request := new(tablestore.SQLQueryRequest)
	request.Query = "drop mapping table test_http_query"
	response, err := client.SQLQuery(request)
	if err != nil {
		println("[Info]: drop mapping tables failed with error: ", err.Error())
		return
	}
	println("[Info]: drop mapping success, request id: ", response.RequestId)
	println("END SQLDropMappingTableSample")
}

// SQLCreateTableSample 目前Create Table创建的是mapping映射表
func SQLCreateTableSample(client *tablestore.TableStoreClient) {
	println("BEGIN SQLCreateTableSample")
	request := new(tablestore.SQLQueryRequest)
	request.Query = "create table if not exists test_http_query (a bigint, b double, c mediumtext, d mediumblob, e bool, primary key (`a`));"
	response, err := client.SQLQuery(request)
	if err != nil {
		println("[Info]: create table failed with error: ", err.Error())
		return
	}
	println("[Info]: create table success, request id: ", response.RequestId)
	println("END SQLCreateTableSample")
}

func SQLDescribeTableSample(client *tablestore.TableStoreClient) {
	println("BEGIN SQLCreateTableSample")
	request := new(tablestore.SQLQueryRequest)
	request.Query = "describe test_http_query;"
	response, err := client.SQLQuery(request)
	if err != nil {
		println("[Info]: describe table failed with error: ", err.Error())
		return
	}

	resultSet := response.ResultSet
	columns := resultSet.Columns()
	for resultSet.HasNext() {
		row := resultSet.Next()
		for i := 0; i < len(columns); i++ {
			name := columns[i].Name
			println(row.GetString(i))
			println(row.GetStringByName(name))
		}
	}
	println("[Info]: describe table success, request id: ", response.RequestId)
	println("END SQLCreateTableSample")
}

func SQLSelectSample(client *tablestore.TableStoreClient) {
	println("BEGIN SQLSelectSample")
	request := new(tablestore.SQLQueryRequest)
	request.Query = "select * from test_http_query;"
	response, err := client.SQLQuery(request)
	if err != nil {
		println("[Info]: select failed with error: ", err.Error())
		return
	}

	resultSet := response.ResultSet
	columns := resultSet.Columns()
	for resultSet.HasNext() {
		row := resultSet.Next()
		for i := 0; i < len(columns); i++ {
			name := columns[i].Name
			println("columnName: ", name)
			isnull, err := row.IsNull(i)
			if err != nil {
				println("[INFO:] get column error, name: ", name, ", error: ", err.Error())
				continue
			}
			if isnull {
				println("[INFO]: column is SQL NULL, name: ", name)
				continue
			}
			switch columns[i].Type {
			case tablestore.ColumnType_STRING:
				println(row.GetString(i))
				println(row.GetStringByName(name))
			case tablestore.ColumnType_INTEGER:
				println(row.GetInt64(i))
				println(row.GetInt64ByName(name))
			case tablestore.ColumnType_DOUBLE:
				println(row.GetFloat64(i))
				println(row.GetFloat64ByName(name))
			case tablestore.ColumnType_BINARY:
				println(row.GetBytes(i))
				println(row.GetBytesByName(name))
			case tablestore.ColumnType_BOOLEAN:
				println(row.GetBool(i))
				println(row.GetBoolByName(name))
			}
		}
	}
	println("END SQLSelectSample")
}
