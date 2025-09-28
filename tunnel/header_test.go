package tunnel

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/common"
	. "gopkg.in/check.v1"
	"log"
	"net/http"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func TestHeader(t *testing.T) {
	TestingT(t)
}

type HeaderSuite struct{}

var _ = Suite(&HeaderSuite{})

var (
	accessKeyID     = "credentials_test_id"
	accessKeySecret = "credentials_test_secret"
	securityToken   = ""
	region          = "cn-hangzhou"
	signingDate     = "20240808"

	uri    = "/ListTable"
	method = "POST"

	date = "2024-08-08T03:22:47.8123Z"

	userAgentTest  = "aliyun-tablestore-sdk-golang/4.0.2"
	apiVersionTest = "2015-12-31"
	instanceName   = "credentials_test"
	contentmd5     = "1B2M2Y8AsgTpgAmY7PhCfg=="

	finalSigningKeyString = "RNN1/s8/m45SdD+EzXlvpyGxc4Enu0RiODs1Lg+xFpo="

	sign   = "A940TpusldxW4mUEOFbhrtwctoU="
	signV4 = "mUSbIuIfN/JuO4/mCOaQZ72JHDc8z6gjPdBpPyAd/ac="
)

func GetReqHeader() http.Header {
	headers := http.Header{}
	headers.Set("User-Agent", userAgentTest)
	headers.Set("x-ots-date", date)
	headers.Set("x-ots-apiversion", apiVersionTest)
	headers.Set("x-ots-accesskeyid", accessKeyID)
	headers.Set("x-ots-instancename", instanceName)
	headers.Set("x-ots-contentmd5", contentmd5)
	return headers
}

func (s *HeaderSuite) TestGetSignature(c *C) {
	log.Println("TestGetSignature started")

	defaultCredentialsProvider := common.DefaultCredentialsProvider{AccessKeyID: accessKeyID, AccessKeySecret: accessKeySecret, SecurityToken: securityToken}
	akInfo := defaultCredentialsProvider.GetCredentials()

	headers := GetReqHeader()

	curSign, err := GetSignature(uri, method, akInfo, headers)

	c.Assert(curSign, Equals, sign)
	c.Assert(err, Equals, nil)

	log.Println("TestGetSignature finished")
}

func (s *HeaderSuite) TestGetSignatureV4(c *C) {
	log.Println("TestGetSignatureV4 started")

	defaultCredentialsProvider := common.V4Credentials{AccessKeyID: accessKeyID, V4SigningStsToken: securityToken, Region: region, SigningDate: signingDate, V4SigningAccessKey: finalSigningKeyString}
	akInfo := defaultCredentialsProvider.GetCredentials()

	headers := GetReqHeader()
	headers.Set("x-ots-signregion", region)
	headers.Set("x-ots-signdate", signingDate)

	curSign, err := GetSignature(uri, method, akInfo, headers)

	c.Assert(curSign, Equals, signV4)
	c.Assert(err, Equals, nil)

	log.Println("TestGetSignatureV4 finished")
}
