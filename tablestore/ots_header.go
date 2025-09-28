package tablestore

import (
	"encoding/base64"
	"github.com/aliyun/aliyun-tablestore-go-sdk/common"
	"hash"
	"net/http"
	"reflect"
	"sort"
	"strings"
)

const (
	xOtsDate                    = "x-ots-date"
	xOtsApiversion              = "x-ots-apiversion"
	xOtsAccesskeyid             = "x-ots-accesskeyid"
	xOtsContentmd5              = "x-ots-contentmd5"
	xOtsHeaderStsToken          = "x-ots-ststoken"
	xOtsHeaderChargeAdmin       = "x-ots-charge-for-admin"
	xOtsSignature               = "x-ots-signature"
	xOtsSignatureV4             = "x-ots-signaturev4"
	xOtsRequestCompressType     = "x-ots-request-compress-type"
	xOtsRequestCompressSize     = "x-ots-request-compress-size"
	xOtsResponseCompressTye     = "x-ots-response-compress-type"
	xOtsHeaderSDKTraceID        = "x-ots-sdk-traceid"
	xOtsHeaderRequestPriority   = "x-ots-request-priority"
	xOtsHeaderRequestTag        = "x-ots-request-tag"
	xOtsHeaderRequestSearchTag  = "x-ots-request-search-tag"
	xOtsHeaderSourceIp          = "x-ots-sourceip"
	xOtsHeaderIsSecureTransport = "x-ots-issecuretransport"
	xOtsHeaderTunnelType        = "x-ots-tunnel-type"
	xOtsHeaderPlayerAccountId   = "x-ots-playeraccountid"
	XOtsHeaderAdminTaskType     = "x-ots-admin-task-type"
	XOtsHeaderAdminTargetUserId = "x-ots-admin-target-userid"
	xOtsPrefix                  = "x-ots"
	xOtsSignRegion              = "x-ots-signregion"
	xOtsSignDate                = "x-ots-signdate"
)

type otsHeader struct {
	name  string
	value string
	must  bool // must is used to check whether the header is valid before signing. If must is true, it means the header must exist.
}

type otsHeaders struct {
	headers []*otsHeader
	hMac    hash.Hash
}

func createOtsHeaders(isV4 bool) *otsHeaders {
	h := new(otsHeaders)

	h.headers = []*otsHeader{
		&otsHeader{name: xOtsDate, must: true},
		&otsHeader{name: xOtsApiversion, must: true},
		&otsHeader{name: xOtsAccesskeyid, must: true},
		&otsHeader{name: xOtsContentmd5, must: true},
		&otsHeader{name: xOtsInstanceName, must: true},
		&otsHeader{name: xOtsSignature, must: false},   // Before signing, the xOtsSignature header should not exist.
		&otsHeader{name: xOtsSignatureV4, must: false}, // Before signing, the xOtsSignatureV4 header should not exist.
		&otsHeader{name: xOtsRequestCompressSize, must: false},
		&otsHeader{name: xOtsResponseCompressTye, must: false},
		&otsHeader{name: xOtsRequestCompressType, must: false},
		&otsHeader{name: xOtsHeaderStsToken, must: false},
		&otsHeader{name: xOtsHeaderChargeAdmin, must: false},
		&otsHeader{name: xOtsHeaderTunnelType, must: false},
		&otsHeader{name: xOtsHeaderPlayerAccountId, must: false},
		&otsHeader{name: XOtsHeaderAdminTaskType, must: false},
		&otsHeader{name: XOtsHeaderAdminTargetUserId, must: false},
		&otsHeader{name: xOtsHeaderSDKTraceID, must: false},
		&otsHeader{name: xOtsHeaderRequestPriority, must: false},
		&otsHeader{name: xOtsHeaderRequestTag, must: false},
		&otsHeader{name: xOtsHeaderRequestSearchTag, must: false},
		&otsHeader{name: xOtsHeaderSourceIp, must: false},
		&otsHeader{name: xOtsHeaderIsSecureTransport, must: false},
		&otsHeader{name: xOtsSignRegion, must: isV4},
		&otsHeader{name: xOtsSignDate, must: isV4},
	}

	sort.Sort(h)

	return h
}

func (h *otsHeaders) Len() int {
	return len(h.headers)
}

func (h *otsHeaders) Swap(i, j int) {
	h.headers[i], h.headers[j] = h.headers[j], h.headers[i]
}

func (h *otsHeaders) Less(i, j int) bool {
	return h.headers[i].name < h.headers[j].name
}

func (h *otsHeaders) search(name string) *otsHeader {
	index := sort.Search(len(h.headers)-1, func(i int) bool {
		return h.headers[i].name >= name
	})

	if index >= len(h.headers) {
		return nil
	}

	return h.headers[index]
}

func (h *otsHeaders) set(name, value string) {
	header := h.search(name)
	if header == nil {
		return
	}

	header.value = value
}

func (h *otsHeaders) signature(uri, method, accessKey string) (string, error) {
	h.hMac = common.GetHmac("HmacSHA1", []byte(accessKey))

	// StringToSign = CanonicalURI + '\n' + HTTPRequestMethod + '\n' + CanonicalQueryString + '\n' + CanonicalHeaders + '\n'
	// TODO CanonicalQueryString is empty
	stringToSign := uri + "\n" + method + "\n" + "\n"

	for _, header := range h.headers {
		if header.must && header.value == "" {
			return "", errMissMustHeader(header.name)
		}
		if header.value != "" {
			stringToSign = stringToSign + header.name + ":" + strings.TrimSpace(header.value) + "\n"
		}
	}

	h.hMac.Reset()
	h.hMac.Write([]byte(stringToSign))

	sign := base64.StdEncoding.EncodeToString(h.hMac.Sum(nil))
	h.set(xOtsSignature, sign)
	return sign, nil
}

func (h *otsHeaders) signatureV4(uri, method, accessKey string) (string, error) {
	h.hMac = common.GetHmac("HmacSHA256", []byte(accessKey))

	// StringToSign = CanonicalURI + '\n' + HTTPRequestMethod + '\n' + CanonicalQueryString + '\n' + CanonicalHeaders + '\n'
	// TODO CanonicalQueryString is empty
	stringToSign := uri + "\n" + method + "\n" + "\n"

	for _, header := range h.headers {
		if header.must && header.value == "" {
			return "", errMissMustHeader(header.name)
		}
		if header.value != "" {
			stringToSign = stringToSign + header.name + ":" + strings.TrimSpace(header.value) + "\n"
		}
	}

	h.hMac.Reset()
	h.hMac.Write([]byte(stringToSign + common.V4_SIGNATURE_SALT))

	sign := base64.StdEncoding.EncodeToString(h.hMac.Sum(nil))
	h.set(xOtsSignatureV4, sign)
	return sign, nil
}

func isCredentialsV4(credentials common.Credentials) bool {
	return reflect.TypeOf(credentials).Implements(reflect.TypeOf((*common.CredentialsV4)(nil)).Elem())
}

func AddExtraHeader(hreq *http.Request, akInfo common.Credentials) {
	if isCredentialsV4(akInfo) {
		hreq.Header.Set(xOtsSignRegion, akInfo.(common.CredentialsV4).GetRegion())
		hreq.Header.Set(xOtsSignDate, akInfo.(common.CredentialsV4).GetSigningDate())
	}
}

func AddSignatureHeader(hreq *http.Request, akInfo common.Credentials, sign string) {
	if isCredentialsV4(akInfo) {
		hreq.Header.Set(xOtsSignatureV4, sign)
	} else {
		hreq.Header.Set(xOtsSignature, sign)
	}
}

func GetSignature(uri string, method string, akInfo common.Credentials, headers http.Header) (string, error) {
	isV4 := isCredentialsV4(akInfo)
	curOtsHeaders := createOtsHeaders(isV4)

	for key, values := range headers {
		lowerKey := strings.ToLower(key)
		if strings.HasPrefix(lowerKey, xOtsPrefix) && len(values) == 1 {
			curOtsHeaders.set(lowerKey, values[0])
		}
	}

	// Sign process based on the credentials version
	var sign string
	var err error
	if isV4 {
		akInfoV4 := akInfo.(common.CredentialsV4)
		curOtsHeaders.set(xOtsSignRegion, akInfoV4.GetRegion())
		curOtsHeaders.set(xOtsSignDate, akInfoV4.GetSigningDate())
		sign, err = curOtsHeaders.signatureV4(uri, method, akInfoV4.GetAccessKeySecret())
	} else {
		sign, err = curOtsHeaders.signature(uri, method, akInfo.GetAccessKeySecret())
	}

	return sign, err
}
