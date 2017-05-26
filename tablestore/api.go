package tablestore

import (
	"fmt"
	"time"
	"bytes"
	"net/http"
	"crypto/md5"
	"encoding/base64"
	"github.com/golang/protobuf/proto"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/tsprotocol"
	"net"
	"math/rand"
)

const (
	createTableUri = "/CreateTable"
	listTableUri = "/ListTable"
	deleteTableUri = "/DeleteTable"
	describeTableUri = "/DescribeTable"
	updateTableUri = "/UpdateTable"
	putRowUri = "/PutRow"
	deleteRowUri = "/DeleteRow"
	getRowUri = "/GetRow"
	updateRowUri = "/UpdateRow"
	batchGetRowUri = "/BatchGetRow"
	batchWriteRowUri = "/BatchWriteRow"
	getRangeUri = "/GetRange"
)

// Constructor: to create the client of OTS service.
// 构造函数：创建OTS服务的客户端。
//
// @param endPoint The address of OTS service. OTS服务地址。
// @param instanceName
// @param accessId The Access ID. 用于标示用户的ID。
// @param accessKey The Access Key. 用于签名和验证的密钥。
// @param options set client config
func NewClient(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...ClientOption) *TableStoreClient {
	client := NewClientWithConfig(endPoint, instanceName, accessKeyId, accessKeySecret, "", nil)
	// client options parse
	for _, option := range options {
		option(client)
	}

	return client
}

type GetHttpClient func() IHttpClient

var currentGetHttpClientFunc GetHttpClient = func() IHttpClient {
	return &TableStoreHttpClient{}
}

// Constructor: to create the client of OTS service. 传入config
// 构造函数：创建OTS服务的客户端。
func NewClientWithConfig(endPoint, instanceName, accessKeyId, accessKeySecret string, securityToken string, config *TableStoreConfig) *TableStoreClient {
	tableStoreClient := new(TableStoreClient)
	tableStoreClient.endPoint = endPoint
	tableStoreClient.instanceName = instanceName
	tableStoreClient.accessKeyId = accessKeyId
	tableStoreClient.accessKeySecret = accessKeySecret
	tableStoreClient.securityToken = securityToken
	if config != nil {
		tableStoreClient.config = config
	} else {
		tableStoreClient.config = getTableStoreDefaultConfig()
	}
	tableStoreTransportProxy := &http.Transport{
		MaxIdleConnsPerHost:   2000,
		Dial: (&net.Dialer{
			Timeout:   tableStoreClient.config.HTTPTimeout.ConnectionTimeout,
		}).Dial,
	}

	tableStoreClient.httpClient = currentGetHttpClientFunc()

	httpClient := &http.Client{
		Transport:tableStoreTransportProxy,
		Timeout: tableStoreClient.config.HTTPTimeout.RequestTimeout,
	}
	tableStoreClient.httpClient.New(httpClient)

	tableStoreClient.random = rand.New(rand.NewSource(time.Now().Unix()))

	return tableStoreClient
}

// 请求服务端
func (tableStoreClient *TableStoreClient) doRequestWithRetry(uri string, req, resp proto.Message) error {
	end := time.Now().Add(tableStoreClient.config.MaxRetryTime)
	url := fmt.Sprintf("%s%s", tableStoreClient.endPoint, uri)
	/* request body */
	var body []byte
	var err error
	if req != nil {
		body, err = proto.Marshal(req)
		if err != nil {
			return err
		}
	} else {
		body = nil;
	}

	var value int64
	var i uint
	var respBody []byte
	for i = 0; ; i++ {
		var statusCode int
		var requestId string
		respBody, err, statusCode, requestId = tableStoreClient.doRequest(url, uri, body, resp)

		if err == nil {
			break
		} else {

			if len(respBody) <= 0 {
				return err
			}
			e := new(tsprotocol.Error)
			errn := proto.Unmarshal(respBody, e)

			value = getNextPause(tableStoreClient, errn, e, i, end, value, uri, statusCode)

			// fmt.Println("hit retry", uri, err, *e.Code, value)
			if value <= 0 {
				if errn != nil {
					return fmt.Errorf("decode resp failed: %s: %s: %s %s", errn, err, string(respBody), requestId)
				} else {
					return fmt.Errorf("%s %s %s", *e.Code, *e.Message ,requestId)
				}
			}

			time.Sleep(time.Duration(value) * time.Millisecond)
		}
	}

	if respBody == nil || len(respBody) == 0 {
		return nil
	}

	err = proto.Unmarshal(respBody, resp)
	if err != nil {
		return fmt.Errorf("decode resp failed: %s", err)
	}

	return nil
}

func getNextPause(tableStoreClient *TableStoreClient, err error, serverError *tsprotocol.Error, count uint, end time.Time, lastInterval int64, action string, statusCode int) int64 {
	if tableStoreClient.config.RetryTimes <= count || time.Now().After(end) {
		return 0
	} else if err == nil && !shouldRetry(*serverError.Code, *serverError.Message, action, statusCode) {
		return 0
	} else {
		value := lastInterval * 2 + tableStoreClient.random.Int63n(DefaultRetryInterval - 1) + 1
		if value > MaxRetryInterval {
			return MaxRetryInterval
		}

		return value
	}
}

func shouldRetry(errorCode string, errorMsg string, action string, httpStatus int) bool {
	if retryNotMatterActions(errorCode, errorMsg) == true {
		return true;
	}

	serverError := httpStatus >= 500 && httpStatus <= 599;
	if (isIdempotent(action) &&
		( errorCode == STORAGE_TIMEOUT || errorCode == INTERNAL_SERVER_ERROR || errorCode == SERVER_UNAVAILABLE || serverError)) {
		return true;
	}
	return false;
}

func retryNotMatterActions(errorCode string, errorMsg string) bool {
	if (errorCode == ROW_OPERATION_CONFLICT || errorCode == NOT_ENOUGH_CAPACITY_UNIT ||
		errorCode == TABLE_NOT_READY || errorCode == PARTITION_UNAVAILABLE ||
		errorCode == SERVER_BUSY || (errorCode == QUOTA_EXHAUSTED && errorMsg == "Too frequent table operations.")) {
		return true;
	} else {
		return false;
	}
}

func isIdempotent(action string) bool {
	if (action == batchGetRowUri || action == describeTableUri ||
		action == getRangeUri || action == getRowUri ||
		action == listTableUri) {
		return true;
	} else {
		return false;
	}
}

func (tableStoreClient *TableStoreClient) doRequest(url string, uri string, body []byte, resp proto.Message) ([]byte, error, int, string) {
	hreq, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err, 0, ""
	}
	/* set headers */
	hreq.Header.Set("User-Agent", "skyeye")

	date := time.Now().UTC().Format(xOtsDateFormat)

	hreq.Header.Set(xOtsDate, date)
	hreq.Header.Set(xOtsApiversion, ApiVersion)
	hreq.Header.Set(xOtsAccesskeyid, tableStoreClient.accessKeyId)
	hreq.Header.Set(xOtsInstanceName, tableStoreClient.instanceName)

	md5Byte := md5.Sum(body)
	md5Base64 := base64.StdEncoding.EncodeToString(md5Byte[:16])
	hreq.Header.Set(xOtsContentmd5, md5Base64)

	otshead := createOtsHeaders(tableStoreClient.accessKeySecret)
	otshead.set(xOtsDate, date)
	otshead.set(xOtsApiversion, ApiVersion)
	otshead.set(xOtsAccesskeyid, tableStoreClient.accessKeyId)
	if tableStoreClient.securityToken != "" {
		hreq.Header.Set(xOtsHeaderStsToken, tableStoreClient.securityToken)
		otshead.set(xOtsHeaderStsToken, tableStoreClient.securityToken)
	}
	otshead.set(xOtsContentmd5, md5Base64)
	otshead.set(xOtsInstanceName, tableStoreClient.instanceName)
	sign, err := otshead.signature(uri, "POST", tableStoreClient.accessKeySecret)

	if err != nil { return nil, err, 0 , ""}
	hreq.Header.Set(xOtsSignature, sign)

	/* end set headers */
	return tableStoreClient.postReq(hreq, url)
}

// table API
// Create a table with the CreateTableRequest, in which the table name and
// primary keys are required.
// 根据CreateTableRequest创建一个表，其中表名和主健列是必选项
//
// @param request of CreateTableRequest.
// @return Void. 无返回值。
func (tableStoreClient *TableStoreClient) CreateTable(request *CreateTableRequest) (*CreateTableResponse, error) {
	if len(request.TableMeta.TableName) > maxTableNameLength {
		return nil, errTableNameTooLong(request.TableMeta.TableName)
	}

	if len(request.TableMeta.SchemaEntry) > maxPrimaryKeyNum {
		return nil, errPrimaryKeyTooMuch
	}

	if len(request.TableMeta.SchemaEntry) == 0 {
		return nil, errCreateTableNoPrimaryKey
	}

	req := new(tsprotocol.CreateTableRequest)
	req.TableMeta = new(tsprotocol.TableMeta)
	req.TableMeta.TableName = proto.String(request.TableMeta.TableName)

	for _, key := range (request.TableMeta.SchemaEntry) {
		keyType := tsprotocol.PrimaryKeyType(*key.Type)
		if key.Option != nil {
			keyOption := tsprotocol.PrimaryKeyOption(*key.Option)
			req.TableMeta.PrimaryKey = append(req.TableMeta.PrimaryKey, &tsprotocol.PrimaryKeySchema{Name: key.Name, Type: &keyType, Option: &keyOption })
		} else {
			req.TableMeta.PrimaryKey = append(req.TableMeta.PrimaryKey, &tsprotocol.PrimaryKeySchema{Name: key.Name, Type: &keyType})
		}
	}

	req.ReservedThroughput = new(tsprotocol.ReservedThroughput)
	req.ReservedThroughput.CapacityUnit = new(tsprotocol.CapacityUnit)
	req.ReservedThroughput.CapacityUnit.Read = proto.Int32(int32(request.ReservedThroughput.Readcap))
	req.ReservedThroughput.CapacityUnit.Write = proto.Int32(int32(request.ReservedThroughput.Writecap))

	req.TableOptions = new(tsprotocol.TableOptions)
	req.TableOptions.TimeToLive = proto.Int32(int32(request.TableOption.TimeToAlive))
	req.TableOptions.MaxVersions = proto.Int32(int32(request.TableOption.MaxVersion))

	resp := new(tsprotocol.CreateTableResponse)
	response := &CreateTableResponse{}
	if err := tableStoreClient.doRequestWithRetry(createTableUri, req, resp); err != nil {
		return nil, err
	}

	return response, nil
}

// List all tables. If done, all table names will be returned.
// 列出所有的表，如果操作成功，将返回所有表的名称。
//
// @param tableNames The returned table names. 返回的表名集合。
// @return Void. 无返回值。
func (tableStoreClient *TableStoreClient) ListTable() (*ListTableResponse, error) {
	resp := new(tsprotocol.ListTableResponse)

	if err := tableStoreClient.doRequestWithRetry(listTableUri, nil, resp); err != nil {
		return &ListTableResponse{}, err
	}

	response := &ListTableResponse{resp.TableNames}
	return response, nil
}

// Delete a table and all its views will be deleted.
// 删除一个表
//
// @param tableName The table name. 表名。
// @return Void. 无返回值。
func (tableStoreClient *TableStoreClient) DeleteTable(request *DeleteTableRequest) (*DeleteTableResponse, error) {
	req := new(tsprotocol.DeleteTableRequest)
	req.TableName = proto.String(request.TableName)

	response := &DeleteTableResponse{}
	if err := tableStoreClient.doRequestWithRetry(deleteTableUri, req, nil); err != nil {
		return nil, err
	}
	return response, nil
}

// Query the tablemeta, tableoption and reservedthroughtputdetails
// @param DescribeTableRequest
// @param DescribeTableResponse
func (tableStoreClient *TableStoreClient) DescribeTable(request *DescribeTableRequest) (*DescribeTableResponse, error) {
	req := new(tsprotocol.DescribeTableRequest)
	req.TableName = proto.String(request.TableName)

	resp := new(tsprotocol.DescribeTableResponse)

	if err := tableStoreClient.doRequestWithRetry(describeTableUri, req, resp); err != nil {
		return &DescribeTableResponse{}, err
	}

	response := new(DescribeTableResponse)
	response.ReservedThroughput = &ReservedThroughput{Readcap: int(*(resp.ReservedThroughputDetails.CapacityUnit.Read)), Writecap: int(*(resp.ReservedThroughputDetails.CapacityUnit.Write))}

	responseTableMeta := new(TableMeta)
	responseTableMeta.TableName = *resp.TableMeta.TableName

	for _, key := range (resp.TableMeta.PrimaryKey) {
		keyType := PrimaryKeyType(*key.Type)

		// enable it when we support kep option in describe table
		/*if key.Option != nil {
			keyOption := PrimaryKeyOption(*key.Option)
			responseTableMeta.SchemaEntry = append(responseTableMeta.SchemaEntry, &PrimaryKeySchema{Name: key.Name, Type: &keyType, Option: &keyOption })
		} else {
			responseTableMeta.SchemaEntry = append(responseTableMeta.SchemaEntry, &PrimaryKeySchema{Name: key.Name, Type: &keyType })
		}*/
		responseTableMeta.SchemaEntry = append(responseTableMeta.SchemaEntry, &PrimaryKeySchema{Name: key.Name, Type: &keyType })
	}
	response.TableMeta = responseTableMeta
	response.TableOption = &TableOption{TimeToAlive: int(*resp.TableOptions.TimeToLive), MaxVersion: int(*resp.TableOptions.MaxVersions)}
	return response, nil
}

// Update the table info includes tableoptions and reservedthroughput
// @param UpdateTableRequest
// @param UpdateTableResponse
func (tableStoreClient *TableStoreClient) UpdateTable(request *UpdateTableRequest) (*UpdateTableResponse, error) {
	req := new(tsprotocol.UpdateTableRequest)
	req.TableName = proto.String(request.TableName)

	if (request.ReservedThroughput != nil) {
		req.ReservedThroughput = new(tsprotocol.ReservedThroughput)
		req.ReservedThroughput.CapacityUnit = new(tsprotocol.CapacityUnit)
		req.ReservedThroughput.CapacityUnit.Read = proto.Int32(int32(request.ReservedThroughput.Readcap))
		req.ReservedThroughput.CapacityUnit.Write = proto.Int32(int32(request.ReservedThroughput.Writecap))
	}

	if (request.TableOption != nil) {
		req.TableOptions = new(tsprotocol.TableOptions)
		req.TableOptions.TimeToLive = proto.Int32(int32(request.TableOption.TimeToAlive))
		req.TableOptions.MaxVersions = proto.Int32(int32(request.TableOption.MaxVersion))
	}

	resp := new(tsprotocol.UpdateTableResponse)

	if err := tableStoreClient.doRequestWithRetry(updateTableUri, req, resp); err != nil {
		return &UpdateTableResponse{}, err
	}

	response := new(UpdateTableResponse)
	response.ReservedThroughput = &ReservedThroughput{Readcap: int(*(resp.ReservedThroughputDetails.CapacityUnit.Read)), Writecap: int(*(resp.ReservedThroughputDetails.CapacityUnit.Write))}
	response.TableOption = &TableOption{TimeToAlive: int(*resp.TableOptions.TimeToLive), MaxVersion: int(*resp.TableOptions.MaxVersions)}
	return response, nil
}

// Put or update a row in a table. The operation is determined by CheckingType,
// which has three options: NO, UPDATE, INSERT. The transaction id is optional.
// 插入或更新行数据。操作针对数据的存在性包含三种检查类型：NO(不检查)，UPDATE
// （更新，数据必须存在）和INSERT（插入，数据必须不存在）。事务ID是可选项。
//
// @param builder The builder for putting a row. 插入或更新数据的Builder。
// @return Void. 无返回值。
func (tableStoreClient *TableStoreClient) PutRow(request *PutRowRequest) (*PutRowResponse, error) {
	if request == nil {
		return nil, nil
	}

	if request.PutRowChange == nil {
		return nil, nil
	}

	req := new(tsprotocol.PutRowRequest)
	req.TableName = proto.String(request.PutRowChange.TableName)
	req.Row = request.PutRowChange.Serialize()

	condition := new(tsprotocol.Condition)
	condition.RowExistence = request.PutRowChange.Condition.buildCondition()
	if request.PutRowChange.Condition.ColumnCondition != nil {
		condition.ColumnCondition = request.PutRowChange.Condition.ColumnCondition.Serialize()
	}

	req.Condition = condition

	resp := new(tsprotocol.PutRowResponse)

	if err := tableStoreClient.doRequestWithRetry(putRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &PutRowResponse{ConsumedCapacityUnit: &ConsumedCapacityUnit{}}
	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	return response, nil
}

// Delete row with pk
// @param DeleteRowRequest
func (tableStoreClient *TableStoreClient) DeleteRow(request *DeleteRowRequest) (*DeleteRowResponse, error) {
	req := new(tsprotocol.DeleteRowRequest)
	req.TableName = proto.String(request.DeleteRowChange.TableName)
	req.Condition = request.DeleteRowChange.getCondition()
	req.PrimaryKey = request.DeleteRowChange.PrimaryKey.Build(true)
	resp := new(tsprotocol.DeleteRowResponse)

	if err := tableStoreClient.doRequestWithRetry(deleteRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &DeleteRowResponse{ConsumedCapacityUnit: &ConsumedCapacityUnit{}}
	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	return response, nil
}

// row API
// Get the data of a row or some columns.
//
// @param getrowrequest
func (tableStoreClient *TableStoreClient) GetRow(request *GetRowRequest) (*GetRowResponse, error) {
	req := new(tsprotocol.GetRowRequest)
	resp := new(tsprotocol.GetRowResponse)

	req.TableName = proto.String(request.SingleRowQueryCriteria.TableName)

	if (request.SingleRowQueryCriteria.getColumnsToGet() != nil) && len(request.SingleRowQueryCriteria.getColumnsToGet()) > 0 {
		req.ColumnsToGet = request.SingleRowQueryCriteria.getColumnsToGet()
	}

	req.PrimaryKey = request.SingleRowQueryCriteria.PrimaryKey.Build(false)

	if request.SingleRowQueryCriteria.MaxVersion != 0 {
		req.MaxVersions = proto.Int32(int32(request.SingleRowQueryCriteria.MaxVersion))
	}

	if request.SingleRowQueryCriteria.TimeRange != nil {
		if (request.SingleRowQueryCriteria.TimeRange.Specific != 0) {
			req.TimeRange = &tsprotocol.TimeRange{SpecificTime : proto.Int64(request.SingleRowQueryCriteria.TimeRange.Specific)}
		} else {
			req.TimeRange = &tsprotocol.TimeRange{StartTime: proto.Int64(request.SingleRowQueryCriteria.TimeRange.Start), EndTime: proto.Int64(request.SingleRowQueryCriteria.TimeRange.End)}
		}
	} else if request.SingleRowQueryCriteria.MaxVersion == 0 {
		return nil, errInvalidInput
	}

	if request.SingleRowQueryCriteria.Filter != nil {
		req.Filter = request.SingleRowQueryCriteria.Filter.Serialize()
	}

	if err := tableStoreClient.doRequestWithRetry(getRowUri, req, resp); err != nil {
		return nil, err
	}


	response := &GetRowResponse{ConsumedCapacityUnit:&ConsumedCapacityUnit{}}
	if len(resp.Row) == 0 {
		return response, nil
	}

	rows, err := readRowsWithHeader(bytes.NewReader(resp.Row))
	if err != nil {
		return nil, err
	}

	for _, pk := range (rows[0].primaryKey) {
		pkColumn := &PrimaryKeyColumn{ColumnName: string(pk.cellName), Value: pk.cellValue.Value}
		response.PrimaryKey.PrimaryKeys = append(response.PrimaryKey.PrimaryKeys, pkColumn)
	}

	for _, cell := range (rows[0].cells) {
		dataColumn := &AttributeColumn{ColumnName: string(cell.cellName), Value: cell.cellValue.Value, Timestamp:cell.cellTimestamp}
		response.Columns = append(response.Columns, dataColumn)
	}

	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	return response, nil
}

// Update row
// @param UpdateRowRequest
func (tableStoreClient *TableStoreClient) UpdateRow(request *UpdateRowRequest) (*UpdateRowResponse, error) {
	req := new(tsprotocol.UpdateRowRequest)
	resp := new(tsprotocol.UpdateRowResponse)

	req.TableName = proto.String(request.UpdateRowChange.TableName)
	req.Condition = request.UpdateRowChange.getCondition()
	req.RowChange = request.UpdateRowChange.Serialize()

	if err := tableStoreClient.doRequestWithRetry(updateRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &UpdateRowResponse{ConsumedCapacityUnit : &ConsumedCapacityUnit{}}
	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	return response, nil
}

// Batch Get Row
// @param BatchGetRowRequest
func (tableStoreClient *TableStoreClient) BatchGetRow(request *BatchGetRowRequest) (*BatchGetRowResponse, error) {
	req := new(tsprotocol.BatchGetRowRequest)

	var tablesInBatch []*tsprotocol.TableInBatchGetRowRequest

	for _, Criteria := range (request.MultiRowQueryCriteria) {
		table := new(tsprotocol.TableInBatchGetRowRequest)
		table.TableName = proto.String(Criteria.TableName)
		table.ColumnsToGet = Criteria.ColumnsToGet

		if Criteria.Filter != nil {
			table.Filter = Criteria.Filter.Serialize()
		}
		if (Criteria.MaxVersion != 0) {
			table.MaxVersions = proto.Int32(int32(Criteria.MaxVersion))
		}

		if Criteria.TimeRange != nil {
			if (Criteria.TimeRange.Specific != 0) {
				table.TimeRange = &tsprotocol.TimeRange{SpecificTime : proto.Int64(Criteria.TimeRange.Specific)}
			} else {
				table.TimeRange = &tsprotocol.TimeRange{StartTime: proto.Int64(Criteria.TimeRange.Start), EndTime: proto.Int64(Criteria.TimeRange.End)}
			}
		} else if Criteria.MaxVersion == 0 {
			return nil, errInvalidInput
		}

		for _, pk := range (Criteria.PrimaryKey) {
			pkWithBytes := pk.Build(false)
			table.PrimaryKey = append(table.PrimaryKey, pkWithBytes)
		}

		tablesInBatch = append(tablesInBatch, table)
	}

	req.Tables = tablesInBatch
	resp := new(tsprotocol.BatchGetRowResponse)

	if err := tableStoreClient.doRequestWithRetry(batchGetRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &BatchGetRowResponse{TableToRowsResult:make(map[string][]RowResult) }

	for _, table := range (resp.Tables) {
		index := int32(0)
		for _, row := range (table.Rows) {
			rowResult := &RowResult{TableName: *table.TableName, IsSucceed: *row.IsOk, ConsumedCapacityUnit : &ConsumedCapacityUnit{}, Index: index}
			index++
			if *row.IsOk == false {
				rowResult.Error = Error{Code: *row.Error.Code, Message: *row.Error.Message }
			} else {
				// len == 0 means row not exist
				if (len(row.Row) > 0) {
					rows, err := readRowsWithHeader(bytes.NewReader(row.Row))
					if err != nil {
						return nil, err
					}

					for _, pk := range (rows[0].primaryKey) {
						pkColumn := &PrimaryKeyColumn{ColumnName: string(pk.cellName), Value: pk.cellValue.Value}
						rowResult.PrimaryKey.PrimaryKeys = append(rowResult.PrimaryKey.PrimaryKeys, pkColumn)
					}

					for _, cell := range (rows[0].cells) {
						dataColumn := &AttributeColumn{ColumnName: string(cell.cellName), Value: cell.cellValue.Value, Timestamp:cell.cellTimestamp}
						rowResult.Columns = append(rowResult.Columns, dataColumn)
					}
				}

				rowResult.ConsumedCapacityUnit.Read = *row.Consumed.CapacityUnit.Read
				rowResult.ConsumedCapacityUnit.Write = *row.Consumed.CapacityUnit.Write
			}

			response.TableToRowsResult[*table.TableName] = append(response.TableToRowsResult[*table.TableName], *rowResult)
		}

	}
	return response, nil
}

// Batch Write Row
// @param BatchWriteRowRequest
func (tableStoreClient *TableStoreClient) BatchWriteRow(request *BatchWriteRowRequest) (*BatchWriteRowResponse, error) {
	req := new(tsprotocol.BatchWriteRowRequest)

	var tablesInBatch []*tsprotocol.TableInBatchWriteRowRequest

	for key, value := range (request.RowChangesGroupByTable) {
		table := new(tsprotocol.TableInBatchWriteRowRequest)
		table.TableName = proto.String(key)

		for _, row := range (value) {
			rowInBatch := &tsprotocol.RowInBatchWriteRowRequest{}
			rowInBatch.Condition = row.getCondition()
			rowInBatch.RowChange = row.Serialize()
			rowInBatch.Type = row.getOperationType().Enum()
			table.Rows = append(table.Rows, rowInBatch)
		}

		tablesInBatch = append(tablesInBatch, table)
	}

	req.Tables = tablesInBatch

	resp := new(tsprotocol.BatchWriteRowResponse)

	if err := tableStoreClient.doRequestWithRetry(batchWriteRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &BatchWriteRowResponse{TableToRowsResult:make(map[string][]RowResult) }

	for _, table := range (resp.Tables) {
		index := int32(0)
		for _, row := range (table.Rows) {
			rowResult := &RowResult{TableName: *table.TableName, IsSucceed: *row.IsOk, ConsumedCapacityUnit : &ConsumedCapacityUnit{}, Index: index}
			index++
			if *row.IsOk == false {
				rowResult.Error = Error{Code: *row.Error.Code, Message: *row.Error.Message }
			} else {
				rowResult.ConsumedCapacityUnit.Read = *row.Consumed.CapacityUnit.Read
				rowResult.ConsumedCapacityUnit.Write = *row.Consumed.CapacityUnit.Write
			} /*else {
				rows, err := readRowsWithHeader(bytes.NewReader(row.Row))
				if err != nil {
					return nil, err
				}

				for _, pk := range (rows[0].primaryKey) {
					pkColumn := &PrimaryKeyColumn{ColumnName: string(pk.cellName), Value: pk.cellValue.Value}
					rowResult.PrimaryKey.PrimaryKeys = append(rowResult.PrimaryKey.PrimaryKeys, pkColumn)
				}

				for _, cell := range (rows[0].cells) {
					dataColumn := &DataColumn{ColumnName: string(cell.cellName), Value: cell.cellValue.Value}
					rowResult.Columns = append(rowResult.Columns, dataColumn)
				}

				rowResult.ConsumedCapacityUnit.Read = *row.Consumed.CapacityUnit.Read
				rowResult.ConsumedCapacityUnit.Write = *row.Consumed.CapacityUnit.Write
			}*/

			response.TableToRowsResult[*table.TableName] = append(response.TableToRowsResult[*table.TableName], *rowResult)
		}
	}
	return response, nil
}

// Get Range
// @param GetRangeRequest
func (tableStoreClient *TableStoreClient) GetRange(request *GetRangeRequest) (*GetRangeResponse, error) {
	req := new(tsprotocol.GetRangeRequest)
	req.TableName = proto.String(request.RangeRowQueryCriteria.TableName)
	req.Direction = request.RangeRowQueryCriteria.Direction.ToDirection().Enum()

	if request.RangeRowQueryCriteria.MaxVersion != 0 {
		req.MaxVersions = proto.Int32(request.RangeRowQueryCriteria.MaxVersion)
	}

	if request.RangeRowQueryCriteria.TimeRange != nil {
		if (request.RangeRowQueryCriteria.TimeRange.Specific != 0) {
			req.TimeRange = &tsprotocol.TimeRange{SpecificTime : proto.Int64(request.RangeRowQueryCriteria.TimeRange.Specific)}
		} else {
			req.TimeRange = &tsprotocol.TimeRange{StartTime: proto.Int64(request.RangeRowQueryCriteria.TimeRange.Start), EndTime: proto.Int64(request.RangeRowQueryCriteria.TimeRange.End)}
		}
	} else if request.RangeRowQueryCriteria.MaxVersion == 0 {
		return nil, errInvalidInput
	}

	if request.RangeRowQueryCriteria.Limit != 0 {
		req.Limit = proto.Int32(request.RangeRowQueryCriteria.Limit)
	}

	if (req.ColumnsToGet != nil) && len(req.ColumnsToGet) > 0 {
		req.ColumnsToGet = request.RangeRowQueryCriteria.ColumnsToGet
	}

	if request.RangeRowQueryCriteria.Filter != nil {
		req.Filter = request.RangeRowQueryCriteria.Filter.Serialize()
	}

	req.InclusiveStartPrimaryKey = request.RangeRowQueryCriteria.StartPrimaryKey.Build(false)
	req.ExclusiveEndPrimaryKey = request.RangeRowQueryCriteria.EndPrimaryKey.Build(false)

	resp := new(tsprotocol.GetRangeResponse)

	if err := tableStoreClient.doRequestWithRetry(getRangeUri, req, resp); err != nil {
		return nil, err
	}

	response := &GetRangeResponse{ConsumedCapacityUnit:&ConsumedCapacityUnit{}}
	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	if len(resp.NextStartPrimaryKey) != 0 {
		currentRows, err := readRowsWithHeader(bytes.NewReader(resp.NextStartPrimaryKey))
		if err != nil {
			return nil, err
		}

		response.NextStartPrimaryKey = &PrimaryKey{}
		for _, pk := range (currentRows[0].primaryKey) {
			pkColumn := &PrimaryKeyColumn{ColumnName: string(pk.cellName), Value: pk.cellValue.Value}
			response.NextStartPrimaryKey.PrimaryKeys = append(response.NextStartPrimaryKey.PrimaryKeys, pkColumn)
		}
	}

	if len(resp.Rows) == 0 {
		return response, nil
	}

	rows, err := readRowsWithHeader(bytes.NewReader(resp.Rows))
	if err != nil {
		return response, err
	}

	for _, row := range rows {
		currentRow := &Row{}
		currentpk := new(PrimaryKey)
		for _, pk := range (row.primaryKey) {
			pkColumn := &PrimaryKeyColumn{ColumnName: string(pk.cellName), Value: pk.cellValue.Value}
			currentpk.PrimaryKeys = append(currentpk.PrimaryKeys, pkColumn)
		}

		currentRow.PrimaryKey = currentpk

		for _, cell := range (row.cells) {
			dataColumn := &AttributeColumn{ColumnName: string(cell.cellName), Value: cell.cellValue.Value, Timestamp:cell.cellTimestamp}
			currentRow.Columns = append(currentRow.Columns, dataColumn)
		}

		response.Rows = append(response.Rows, currentRow)
	}

	return response, nil

}


