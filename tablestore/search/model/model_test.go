package model

import (
    "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
    "github.com/stretchr/testify/assert"
    "testing"
)

func TestGeoHashPrecision_ProtoBuffer(t *testing.T) {
    precision := []GeoHashPrecision{GHP_5009KM_4992KM_1, GHP_1252KM_624KM_2, GHP_156KM_156KM_3, GHP_39KM_19KM_4, GHP_4900M_4900M_5, GHP_1200M_609M_6, GHP_152M_152M_7,
        GHP_38M_19M_8, GHP_480CM_480CM_9, GHP_120CM_595MM_10, GHP_149MM_149MM_11, GHP_37MM_19MM_12}
    pbPrecision := []otsprotocol.GeoHashPrecision{otsprotocol.GeoHashPrecision_GHP_5009KM_4992KM_1, otsprotocol.GeoHashPrecision_GHP_1252KM_624KM_2, otsprotocol.GeoHashPrecision_GHP_156KM_156KM_3,
        otsprotocol.GeoHashPrecision_GHP_39KM_19KM_4, otsprotocol.GeoHashPrecision_GHP_4900M_4900M_5, otsprotocol.GeoHashPrecision_GHP_1200M_609M_6, otsprotocol.GeoHashPrecision_GHP_152M_152M_7,
        otsprotocol.GeoHashPrecision_GHP_38M_19M_8, otsprotocol.GeoHashPrecision_GHP_480CM_480CM_9, otsprotocol.GeoHashPrecision_GHP_120CM_595MM_10, otsprotocol.GeoHashPrecision_GHP_149MM_149MM_11,
        otsprotocol.GeoHashPrecision_GHP_37MM_19MM_12}
    for i := 0; i < 12; i++ {
        assert.Equal(t, &pbPrecision[i], precision[i].ProtoBuffer())
    }
}