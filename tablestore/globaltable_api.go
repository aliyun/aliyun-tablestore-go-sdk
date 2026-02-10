package tablestore

import (
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

func (tableStoreClient *TableStoreClient) CreateGlobalTable(request *CreateGlobalTableRequest) (*CreateGlobalTableResponse, error) {
	protoReq := new(otsprotocol.CreateGlobalTableRequest)
	protoReq.BaseTable = &otsprotocol.BaseTable{
		RegionId:     proto.String(request.BaseTable.RegionId),
		InstanceName: proto.String(request.BaseTable.InstanceName),
		TableName:    proto.String(request.BaseTable.TableName),
	}
	protoReq.Placements = make([]*otsprotocol.Placement, 0, len(request.Placements))
	for _, placement := range request.Placements {
		p := &otsprotocol.Placement{
			RegionId:     proto.String(placement.RegionId),
			InstanceName: proto.String(placement.InstanceName),
			Writable:     proto.Bool(placement.Writable),
		}
		protoReq.Placements = append(protoReq.Placements, p)
	}
	switch request.SyncMode {
	case SyncMode_Row:
		protoReq.SyncMode = otsprotocol.SyncMode_SYNC_MODE_ROW.Enum()
	case SyncMode_Column:
		protoReq.SyncMode = otsprotocol.SyncMode_SYNC_MODE_COLUMN.Enum()
	default:
		return nil, fmt.Errorf("SyncMode is invalid: %s", request.SyncMode)
	}

	switch request.ServeMode {
	case ServeMode_PrimarySecondary:
		protoReq.ServeMode = otsprotocol.ServeMode_PRIMARY_SECONDARY.Enum()
	case ServeMode_PeerToPeer:
		protoReq.ServeMode = otsprotocol.ServeMode_PEER_TO_PEER.Enum()
	default:
		return nil, fmt.Errorf("ServeMode is invalid: %s", request.ServeMode)
	}

	protoResp := new(otsprotocol.CreateGlobalTableResponse)
	response := &CreateGlobalTableResponse{}
	if err := tableStoreClient.doRequestWithRetry(createGlobalTableUri, protoReq, protoResp, &response.ResponseInfo, request.ExtraRequestInfo); err != nil {
		return nil, err
	}
	if protoResp.GlobalTableId != nil {
		response.GlobalTableId = *protoResp.GlobalTableId
	}
	return response, nil
}

func (tableStoreClient *TableStoreClient) BindGlobalTable(request *BindGlobalTableRequest) (*BindGlobalTableResponse, error) {
	protoReq := new(otsprotocol.BindGlobalTableRequest)
	protoReq.GlobalTableId = proto.String(request.GlobalTableId)
	protoReq.GlobalTableName = proto.String(request.GlobalTableName)
	protoReq.Placements = make([]*otsprotocol.Placement, 0, len(request.Placements))
	for _, placement := range request.Placements {
		p := &otsprotocol.Placement{
			RegionId:     proto.String(placement.RegionId),
			InstanceName: proto.String(placement.InstanceName),
			Writable:     proto.Bool(placement.Writable),
		}
		protoReq.Placements = append(protoReq.Placements, p)
	}

	protoResp := new(otsprotocol.BindGlobalTableResponse)
	response := &BindGlobalTableResponse{}
	if err := tableStoreClient.doRequestWithRetry(bindGlobalTableUri, protoReq, protoResp, &response.ResponseInfo, request.ExtraRequestInfo); err != nil {
		return nil, err
	}
	return response, nil
}

func (tableStoreClient *TableStoreClient) UnbindGlobalTable(request *UnbindGlobalTableRequest) (*UnbindGlobalTableResponse, error) {
	protoReq := new(otsprotocol.UnbindGlobalTableRequest)
	protoReq.GlobalTableId = proto.String(request.GlobalTableId)
	protoReq.GlobalTableName = proto.String(request.GlobalTableName)
	protoReq.Removals = make([]*otsprotocol.Removal, 0, len(request.Removals))
	for _, removal := range request.Removals {
		r := &otsprotocol.Removal{
			RegionId:     proto.String(removal.RegionId),
			InstanceName: proto.String(removal.InstanceName),
		}
		protoReq.Removals = append(protoReq.Removals, r)
	}

	protoResp := new(otsprotocol.UnbindGlobalTableResponse)
	response := &UnbindGlobalTableResponse{}
	if err := tableStoreClient.doRequestWithRetry(unbindGlobalTableUri, protoReq, protoResp, &response.ResponseInfo, request.ExtraRequestInfo); err != nil {
		return nil, err
	}
	return response, nil
}

func (tableStoreClient *TableStoreClient) DescribeGlobalTable(request *DescribeGlobalTableRequest) (*DescribeGlobalTableResponse, error) {
	protoReq := new(otsprotocol.DescribeGlobalTableRequest)
	protoReq.GlobalTableName = proto.String(request.GlobalTableName)

	if request.GlobalTableId != "" {
		protoReq.GlobalTableId = proto.String(request.GlobalTableId)
	}
	if request.PhyTable != nil {
		protoReq.PhyTable = &otsprotocol.PhyTable{
			RegionId:     proto.String(request.PhyTable.RegionId),
			InstanceName: proto.String(request.PhyTable.InstanceName),
			TableName:    proto.String(request.PhyTable.TableName),
			Writable:     proto.Bool(request.PhyTable.Writable),
		}
	}

	protoResp := new(otsprotocol.DescribeGlobalTableResponse)
	response := &DescribeGlobalTableResponse{}
	if err := tableStoreClient.doRequestWithRetry(describeGlobalTableUri, protoReq, protoResp, &response.ResponseInfo, request.ExtraRequestInfo); err != nil {
		return nil, err
	}
	if protoResp.GlobalTableId != nil {
		response.GlobalTableId = *protoResp.GlobalTableId
	}
	if protoResp.ServeMode != nil {
		switch *protoResp.ServeMode {
		case otsprotocol.ServeMode_PRIMARY_SECONDARY:
			response.ServeMode = ServeMode_PrimarySecondary
		case otsprotocol.ServeMode_PEER_TO_PEER:
			response.ServeMode = ServeMode_PeerToPeer
		}
	}
	if protoResp.Status != nil {
		switch *protoResp.Status {
		case otsprotocol.GlobalTableStatus_G_INIT:
			response.Status = GlobalTableStatus_Init
		case otsprotocol.GlobalTableStatus_G_RE_CONF:
			response.Status = GlobalTableStatus_Reconf
		case otsprotocol.GlobalTableStatus_G_ACTIVE:
			response.Status = GlobalTableStatus_Active
		}
	}

	for _, t := range protoResp.PhyTables {
		table := PhyTable{
			RegionId:     *t.RegionId,
			InstanceName: *t.InstanceName,
			TableName:    *t.TableName,
		}
		if t.Status != nil {
			switch *t.Status {
			case otsprotocol.PhyTableStatus_PHY_PENDING:
				pending := PhyTableStatus_Pending
				table.Status = &pending
			case otsprotocol.PhyTableStatus_PHY_INIT:
				init := PhyTableStatus_Init
				table.Status = &init
			case otsprotocol.PhyTableStatus_PHY_SYNCDATA:
				syncdata := PhyTableStatus_Syncdata
				table.Status = &syncdata
			case otsprotocol.PhyTableStatus_PHY_READY:
				ready := PhyTableStatus_Ready
				table.Status = &ready
			case otsprotocol.PhyTableStatus_PHY_ACTIVE:
				active := PhyTableStatus_Active
				table.Status = &active
			case otsprotocol.PhyTableStatus_PHY_UNBINDING:
				unbinding := PhyTableStatus_Unbinding
				table.Status = &unbinding
			case otsprotocol.PhyTableStatus_PHY_UNBOUND:
				unbound := PhyTableStatus_Unbound
				table.Status = &unbound
			}
		}
		table.StatusTimestamp = t.StatusTimestamp
		table.TableId = t.TableId
		//table.TableHashKey = t.TableHashKey
		//table.Endpoint = t.Endpoint
		table.Role = t.GetRole()
		if t.Stage != nil {
			switch *t.Stage {
			case otsprotocol.SyncStage_SYNC_INIT:
				init := PhyTableSyncStage_Init
				table.Stage = &init
			case otsprotocol.SyncStage_SYNC_FULL:
				full := PhyTableSyncStage_Full
				table.Stage = &full
			case otsprotocol.SyncStage_SYNC_INCR:
				incr := PhyTableSyncStage_Incr
				table.Stage = &incr
			}
		}
		//table.MetaVersion = t.MetaVersion

		response.PhyTables = append(response.PhyTables, &table)
	}
	return response, nil
}

func (tableStoreClient *TableStoreClient) UpdateGlobalTable(request *UpdateGlobalTableRequest) (*UpdateGlobalTableResponse, error) {
	if request.GlobalTableId == "" || request.GlobalTableName == "" {
		return nil, errors.New("GlobalTableId or GlobalTableName is empty")
	}
	upPhyTable := request.PhyTable
	if upPhyTable.RegionId == "" || upPhyTable.InstanceName == "" || upPhyTable.TableName == "" {
		return nil, errors.New("PhyTable's RegionId or InstanceName or TableName is empty")
	}
	protoReq := new(otsprotocol.UpdateGlobalTableRequest)
	protoReq.GlobalTableId = proto.String(request.GlobalTableId)
	protoReq.GlobalTableName = proto.String(request.GlobalTableName)
	protoReq.PhyTable = &otsprotocol.UpdatePhyTable{
		RegionId:     proto.String(upPhyTable.RegionId),
		InstanceName: proto.String(upPhyTable.InstanceName),
		TableName:    proto.String(upPhyTable.TableName),
	}

	changed := false
	if upPhyTable.Writable != nil {
		protoReq.PhyTable.Writable = upPhyTable.Writable
		changed = true
	}
	if upPhyTable.PrimaryEligible != nil {
		protoReq.PhyTable.PrimaryEligible = upPhyTable.PrimaryEligible
		changed = true
	}
	if !changed {
		return nil, errors.New("UpdateGlobalTableRequest does not contain any update item")
	}

	protoResp := new(otsprotocol.UpdateGlobalTableResponse)
	response := &UpdateGlobalTableResponse{}
	if err := tableStoreClient.doRequestWithRetry(updateGlobalTableRwUri, protoReq, protoResp, &response.ResponseInfo, request.ExtraRequestInfo); err != nil {
		return nil, err
	}
	return response, nil
}
