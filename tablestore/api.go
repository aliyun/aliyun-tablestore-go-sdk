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
)

const (
	createTableUri = "/CreateTable"
	listTableUri = "/ListTable"
	deleteTableUri = "/DeleteTable"
	describeTableUri = "/DescribeTable"
	UpdateTableUri = "/UpdateTable"
	PutRowUri = "/PutRow"
	DeleteRowUri = "/DeleteRow"
	GetRowUri = "/GetRow"
	UpdateRowUri = "/UpdateRow"
	BatchGetRowUri = "/BatchGetRow"
	BatchWriteRowUri = "/BatchWriteRow"
)

// Constructor: to create the client of OTS service.
// 构造函数：创建OTS服务的客户端。
//
// @param endPoint The address of OTS service. OTS服务地址。
// @param instanceName
// @param accessId The Access ID. 用于标示用户的ID。
// @param accessKey The Access Key. 用于签名和验证的密钥。
// @param options set client config
func NewClient(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...ClientOption) *TSClient {
	tsClient := new(TSClient)
	tsClient.endPoint = endPoint
	tsClient.instanceName = instanceName
	tsClient.accessKeyId = accessKeyId
	tsClient.accessKeySecret = accessKeySecret
	tsClient.config = getTsDefaultConfig()
	otsTransportProxy := &http.Transport{
		MaxIdleConnsPerHost:   2000,
		Dial: (&net.Dialer{
			Timeout:   tsClient.config.HTTPTimeout.ConnectionTimeout,
		}).Dial,
	}

	tsClient.httpClient = &http.Client{
		Transport:otsTransportProxy,
		Timeout: tsClient.config.HTTPTimeout.RequestTimeout,
	}

	// client options parse
	for _, option := range options {
		option(tsClient)
	}

	return tsClient
}

// 请求服务端
func (ots *TSClient) doRequest(uri string, req, resp proto.Message) error {
	url := fmt.Sprintf("%s%s", ots.endPoint, uri)
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

	var count uint = 0

	retry:

	hreq, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	/* set headers */
	hreq.Header.Set("User-Agent", "skyeye")

	date := time.Now().UTC().Format(xOtsDateFormat)

	hreq.Header.Set(xOtsDate, date)
	hreq.Header.Set(xOtsApiversion, ApiVersion)
	hreq.Header.Set(xOtsAccesskeyid, ots.accessKeyId)
	hreq.Header.Set(xOtsInstanceName, ots.instanceName)

	md5Byte := md5.Sum(body)
	md5Base64 := base64.StdEncoding.EncodeToString(md5Byte[:16])
	hreq.Header.Set(xOtsContentmd5, md5Base64)

	otshead := createOtsHeaders(ots.accessKeySecret)
	otshead.set(xOtsDate, date)
	otshead.set(xOtsApiversion, ApiVersion)
	otshead.set(xOtsAccesskeyid, ots.accessKeyId)
	otshead.set(xOtsContentmd5, md5Base64)
	otshead.set(xOtsInstanceName, ots.instanceName)
	sign, err := otshead.signature(uri, "POST", ots.accessKeySecret)

	if err != nil {
		// fmt.Println("failed to signature")
		return err
	}
	hreq.Header.Set(xOtsSignature, sign)

	/* end set headers */
	body, err = ots.postReq(hreq, url)
	if err != nil {
		if len(body) > 0 {
			e := new(tsprotocol.Error)
			errn := proto.Unmarshal(body, e)

			if errn != nil {
				count++
				if count <= ots.config.RetryTimes {
					goto retry
				}
				return fmt.Errorf("decode resp failed: %s: %s: %s", errn, err, string(body))
			} else {
				switch *e.Code {
				case "OTSServerBusy":
					fallthrough
				case "OTSTimeout":
					time.Sleep(time.Millisecond * 10)
					count++
					if count <= ots.config.RetryTimes {
						goto retry
					}
				}
				return fmt.Errorf("%s", *e.Code)
			}
		}

		return err
	}

	if len(body) == 0 {
		return nil
	}

	err = proto.Unmarshal(body, resp)
	if err != nil {
		return fmt.Errorf("decode resp failed: %s", err)
	}

	return nil
}

// table API
// Create a table with the CreateTableRequest, in which the table name and
// primary keys are required. Views and table group name are optional, but
// they must be assigned at this call if they are needed.
// 根据CreateTableRequest创建一个表，其中表名和主健列是必选项，表组名是
// 可选项（只能此时创建，建表之后无法更改）。
//
// @param request of CreateTableRequest.
// @return Void. 无返回值。
func (ots *TSClient) CreateTable(request *CreateTableRequest) (*CreateTableResponse, error) {
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
	if err := ots.doRequest(createTableUri, req, resp); err != nil {
		return nil, err
	}

	return response, nil
}

// List all tables. If done, all table names will be returned.
// 列出所有的表，如果操作成功，将返回所有表的名称。
//
// @param tableNames The returned table names. 返回的表名集合。
// @return Void. 无返回值。
func (ots *TSClient) ListTable() (*ListTableResponse, error) {
	resp := new(tsprotocol.ListTableResponse)

	if err := ots.doRequest(listTableUri, nil, resp); err != nil {
		return &ListTableResponse{}, nil
	}

	response := &ListTableResponse{resp.TableNames}
	return response, nil
}

// Delete a table and all its views will be deleted.
// 删除一个表，同时它包含的视图也会被删除。
//
// @param tableName The table name. 表名。
// @return Void. 无返回值。
func (ots *TSClient) DeleteTable(request *DeleteTableRequest) (*DeleteTableResponse, error) {
	req := new(tsprotocol.DeleteTableRequest)
	req.TableName = proto.String(request.TableName)

	response := &DeleteTableResponse{}
	if err := ots.doRequest(deleteTableUri, req, nil); err != nil {
		return nil, err
	}
	return response, nil
}

// Query the tablemeta, tableoption and reservedthroughtputdetails
// @param DescribeTableRequest
// @param DescribeTableResponse
func (ots *TSClient) DescribeTable(request *DescribeTableRequest) (*DescribeTableResponse, error) {
	req := new(tsprotocol.DescribeTableRequest)
	req.TableName = proto.String(request.TableName)

	resp := new(tsprotocol.DescribeTableResponse)

	if err := ots.doRequest(describeTableUri, req, resp); err != nil {
		return &DescribeTableResponse{}, err
	}

	response := new(DescribeTableResponse)
	response.ReservedThroughput = &ReservedThroughput{Readcap: int(*(resp.ReservedThroughputDetails.CapacityUnit.Read)), Writecap: int(*(resp.ReservedThroughputDetails.CapacityUnit.Write))}

	responseTableMeta := new(TableMeta)
	responseTableMeta.TableName = *resp.TableMeta.TableName

	for _, key := range (resp.TableMeta.PrimaryKey) {
		keyType := PrimaryKeyType(*key.Type)
		if key.Option != nil {
			keyOption := PrimaryKeyOption(*key.Option)
			responseTableMeta.SchemaEntry = append(responseTableMeta.SchemaEntry, &PrimaryKeySchema{Name: key.Name, Type: &keyType, Option: &keyOption })
		} else {
			responseTableMeta.SchemaEntry = append(responseTableMeta.SchemaEntry, &PrimaryKeySchema{Name: key.Name, Type: &keyType })
		}
	}
	response.TableMeta = responseTableMeta
	response.TableOption = &TableOption{TimeToAlive: int(*resp.TableOptions.TimeToLive), MaxVersion: int(*resp.TableOptions.MaxVersions)}
	return response, nil
}

// Update the table info includes tableoptions and reservedthroughput
// @param UpdateTableRequest
// @param UpdateTableResponse
func (ots *TSClient) UpdateTable(request *UpdateTableRequest) (*UpdateTableResponse, error) {
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

	if err := ots.doRequest(UpdateTableUri, req, resp); err != nil {
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
func (ots *TSClient) PutRow(request *PutRowRequest) (*PutRowResponse, error) {
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

	if err := ots.doRequest(PutRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &PutRowResponse{ConsumedCapacityUnit: &ConsumedCapacityUnit{}}
	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	return response, nil
}

// Delete row with pk
// @param DeleteRowRequest
func (ots *TSClient) DeleteRow(request *DeleteRowRequest) (*DeleteRowResponse, error) {
	req := new(tsprotocol.DeleteRowRequest)
	req.TableName = proto.String(request.DeleteRowChange.TableName)
	req.Condition = request.DeleteRowChange.getCondition()
	req.PrimaryKey = request.DeleteRowChange.PrimaryKey.Build(true)
	resp := new(tsprotocol.DeleteRowResponse)

	if err := ots.doRequest(DeleteRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &DeleteRowResponse{ConsumedCapacityUnit: &ConsumedCapacityUnit{}}
	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	return response, nil
}

// row API
// Get the data of a row or some columns. The transactionId is optional.
// 获取一行数据或部分列数据。事务ID是可选项。
//
// @param builder The builder for getting a single row. 查询单行的Builder。
// @return The iterator of returned row. 查询到的Row智能指针。
func (ots *TSClient) GetRow(request *GetRowRequest) (*GetRowResponse, error) {
	req := new(tsprotocol.GetRowRequest)
	resp := new(tsprotocol.GetRowResponse)

	req.TableName = proto.String(request.SingleRowQueryCriteria.TableName)
	req.ColumnsToGet = request.SingleRowQueryCriteria.getColumnsToGet()

	req.PrimaryKey = request.SingleRowQueryCriteria.PrimaryKey.Build(false)
	req.MaxVersions = proto.Int32(int32(request.SingleRowQueryCriteria.MaxVersion))

	if request.SingleRowQueryCriteria.Filter != nil {
		req.Filter = request.SingleRowQueryCriteria.Filter.Serialize()
	}

	if err := ots.doRequest(GetRowUri, req, resp); err != nil {
		return nil, err
	}

	if len(resp.Row) == 0 {
		return nil, nil
	}

	rows, err := readRowsWithHeader(bytes.NewReader(resp.Row))
	if err != nil {
		return nil, err
	}

	response := &GetRowResponse{ConsumedCapacityUnit:&ConsumedCapacityUnit{}}
	for _, pk := range (rows[0].primaryKey) {
		pkColumn := &PrimaryKeyColumn{ColumnName: string(pk.cellName), Value: pk.cellValue.Value}
		response.PrimaryKey.PrimaryKeys = append(response.PrimaryKey.PrimaryKeys, pkColumn)
	}

	for _, cell := range (rows[0].cells) {
		dataColumn := &DataColumn{ColumnName: string(cell.cellName), Value: cell.cellValue.Value}
		response.Columns = append(response.Columns, dataColumn)
	}

	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	return response, nil
}

// Update row
// @param UpdateRowRequest
func (ots *TSClient) UpdateRow(request *UpdateRowRequest) (*UpdateRowResponse, error) {
	req := new(tsprotocol.UpdateRowRequest)
	resp := new(tsprotocol.UpdateRowResponse)

	req.TableName = proto.String(request.UpdateRowChange.TableName)
	req.Condition = request.UpdateRowChange.getCondition()
	req.RowChange = request.UpdateRowChange.Serialize()

	if err := ots.doRequest(UpdateRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &UpdateRowResponse{ConsumedCapacityUnit : &ConsumedCapacityUnit{}}
	response.ConsumedCapacityUnit.Read = *resp.Consumed.CapacityUnit.Read
	response.ConsumedCapacityUnit.Write = *resp.Consumed.CapacityUnit.Write
	return response, nil
}


func (ots *TSClient) BatchGetRow(request *BatchGetRowRequest) (*BatchGetRowResponse,error) {
	req := new(tsprotocol.BatchGetRowRequest)

	var tablesInBatch []*tsprotocol.TableInBatchGetRowRequest

	for _, Criteria := range(request.MultiRowQueryCriteria){
		table := new(tsprotocol.TableInBatchGetRowRequest)
		table.TableName = proto.String(Criteria.TableName)
		table.ColumnsToGet = Criteria.ColumnsToGet

		if Criteria.Filter !=nil{
			table.Filter = Criteria.Filter.Serialize()
		}
		table.MaxVersions =proto.Int32(int32(Criteria.MaxVersion))


		for _, pk := range (Criteria.PrimaryKey) {
			pkWithBytes := pk.Build(false)
			table.PrimaryKey = append(table.PrimaryKey, pkWithBytes)
		}

		tablesInBatch = append(tablesInBatch, table)
	}

	req.Tables = tablesInBatch
	resp := new(tsprotocol.BatchGetRowResponse)

	if err := ots.doRequest(BatchGetRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &BatchGetRowResponse{TableToRowsResult:make(map[string][]RowResult) }

	for _, table := range (resp.Tables) {
		for _, row := range(table.Rows) {
			rowResult := &RowResult{TableName: *table.TableName, IsSucceed: *row.IsOk, ConsumedCapacityUnit : &ConsumedCapacityUnit{}}
			if *row.IsOk == false {
				rowResult.Error = Error{ Code: *row.Error.Code, Message: *row.Error.Message }
			} else {
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
			}

			response.TableToRowsResult[*table.TableName] = append(response.TableToRowsResult[*table.TableName], *rowResult)
		}


	}
	return response, nil
}

func (ots *TSClient) BatchWriteRow(request *BatchWriteRowRequest) (*BatchWriteRowResponse,error) {
	req := new(tsprotocol.BatchWriteRowRequest)

	var tablesInBatch []*tsprotocol.TableInBatchWriteRowRequest

	for key, value := range(request.RowChangesGroupByTable){
		table := new(tsprotocol.TableInBatchWriteRowRequest)
		table.TableName = proto.String(key)

		for _, row := range (value) {
			rowInBatch :=&tsprotocol.RowInBatchWriteRowRequest{}
			rowInBatch.Condition = row.getCondition()
			rowInBatch.RowChange = row.Serialize()
			rowInBatch.Type = row.getOperationType().Enum()
			table.Rows = append(table.Rows, rowInBatch)
		}

		tablesInBatch = append(tablesInBatch, table)
	}

	req.Tables = tablesInBatch

	resp := new(tsprotocol.BatchWriteRowResponse)

	if err := ots.doRequest(BatchWriteRowUri, req, resp); err != nil {
		return nil, err
	}

	response := &BatchWriteRowResponse{TableToRowsResult:make(map[string][]RowResult) }

	for _, table := range (resp.Tables) {
		for _, row := range(table.Rows) {
			rowResult := &RowResult{TableName: *table.TableName, IsSucceed: *row.IsOk, ConsumedCapacityUnit : &ConsumedCapacityUnit{}}
			if *row.IsOk == false {
				rowResult.Error = Error{ Code: *row.Error.Code, Message: *row.Error.Message }
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

/*func (ots *TSClient) GetRange(request *GetRangeRequest) (*GetRangeResponse,error) {
	req := new(tsprotocol.GetRangeRequest)
	req.TableName = request.RangeRowQueryCriteria.TableName
	req.Direction = request.RangeRowQueryCriteria.Direction.ToDirection()
}*/


