package tablestore

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/common"
	. "gopkg.in/check.v1"
	"log"
	"math/rand"
	"net/http"
	"time"
)

type OtsHeaderSuite struct{}

var _ = Suite(&OtsHeaderSuite{})

var (
	accessKeyID     = "credentials_test_id"
	accessKeySecret = "credentials_test_secret"
	securityToken   = ""
	region          = "cn-hangzhou"
	signingDate     = "20240808"

	uri    = "/ListTable"
	method = "POST"

	date         = "2024-08-08T03:22:47.8123Z"
	apiVersion   = "2015-12-31"
	instanceName = "credentials_test"
	contentmd5   = "1B2M2Y8AsgTpgAmY7PhCfg=="

	finalSigningKeyString = "RNN1/s8/m45SdD+EzXlvpyGxc4Enu0RiODs1Lg+xFpo="

	sign   = "A940TpusldxW4mUEOFbhrtwctoU="
	signV4 = "mUSbIuIfN/JuO4/mCOaQZ72JHDc8z6gjPdBpPyAd/ac="
)

func GetReqHeader() http.Header {
	headers := http.Header{}
	headers.Set("User-Agent", userAgent)
	headers.Set("x-ots-date", date)
	headers.Set("x-ots-apiversion", apiVersion)
	headers.Set("x-ots-accesskeyid", accessKeyID)
	headers.Set("x-ots-instancename", instanceName)
	headers.Set("x-ots-contentmd5", contentmd5)
	return headers
}

func (s *OtsHeaderSuite) TestGetSignature(c *C) {
	log.Println("TestGetSignature started")

	defaultCredentialsProvider := common.DefaultCredentialsProvider{AccessKeyID: accessKeyID, AccessKeySecret: accessKeySecret, SecurityToken: securityToken}
	akInfo := defaultCredentialsProvider.GetCredentials()

	headers := GetReqHeader()

	curSign, err := GetSignature(uri, method, akInfo, headers)

	c.Assert(curSign, Equals, sign)
	c.Assert(err, Equals, nil)

	log.Println("TestGetSignature finished")
}

func (s *OtsHeaderSuite) TestGetSignatureV4(c *C) {
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

func (s *OtsHeaderSuite) TestV2V4CredentialsPerformance(c *C) {
	repeatTimes := 1000000
	accessKeyIDList, accessKeySecretList := prepareData(repeatTimes)
	time.Sleep(1 * time.Second)
	{
		v4TestStartTime := time.Now()
		for i := 0; i < repeatTimes; i++ {
			id, secret := accessKeyIDList[i], accessKeySecretList[i]
			provider := &common.DefaultCredentialsProvider{AccessKeyID: id, AccessKeySecret: secret, SecurityToken: securityToken}
			v4Credentials := common.CreateByCredentials(provider.GetCredentials(), region)
			akInfo := v4Credentials.GetCredentials()

			headers := GetReqHeader()
			headers.Set("x-ots-signregion", region)
			headers.Set("x-ots-signdate", signingDate)

			_, err := GetSignature(uri, method, akInfo, headers)

			c.Assert(err, Equals, nil)
		}
		v4TestEndTime := time.Now()
		fmt.Printf("使用V2签名生成V4签名，并使用V4签名共%d次总用时：%v\n", len(accessKeyIDList), v4TestEndTime.Sub(v4TestStartTime))
	}
	time.Sleep(1 * time.Second)
	{
		v4TestStartTime := time.Now()
		for i := 0; i < repeatTimes; i++ {
			id, secret := accessKeyIDList[i], accessKeySecretList[i]
			v4Credentials := common.V4Credentials{AccessKeyID: id, V4SigningStsToken: securityToken, Region: region, SigningDate: signingDate, V4SigningAccessKey: secret}
			akInfo := v4Credentials.GetCredentials()

			headers := GetReqHeader()
			headers.Set("x-ots-signregion", region)
			headers.Set("x-ots-signdate", signingDate)

			_, err := GetSignature(uri, method, akInfo, headers)

			c.Assert(err, Equals, nil)
		}
		v4TestEndTime := time.Now()
		fmt.Printf("使用派生秘钥构造V4签名，并使用V4签名共%d次总用时：%v\n", len(accessKeyIDList), v4TestEndTime.Sub(v4TestStartTime))
	}
	time.Sleep(1 * time.Second)
	{
		v2TestStartTime := time.Now()
		for i := 0; i < repeatTimes; i++ {
			id, secret := accessKeyIDList[i], accessKeySecretList[i]
			defaultCredentialsProvider := common.DefaultCredentialsProvider{AccessKeyID: id, AccessKeySecret: secret, SecurityToken: securityToken}
			akInfo := defaultCredentialsProvider.GetCredentials()

			headers := GetReqHeader()

			_, err := GetSignature(uri, method, akInfo, headers)

			c.Assert(err, Equals, nil)
		}
		v2TestEndTime := time.Now()
		fmt.Printf("使用V2签名共%d次总用时：%v\n", len(accessKeyIDList), v2TestEndTime.Sub(v2TestStartTime))
	}
}

func prepareData(repeatTimes int) ([]string, []string) {
	accessKeyIDList := make([]string, repeatTimes)
	accessKeySecretList := make([]string, repeatTimes)
	for i := 0; i < repeatTimes; i++ {
		accessKeyIDList[i] = getRandomString(24)
		accessKeySecretList[i] = getRandomString(30)
	}
	return accessKeyIDList, accessKeySecretList
}

func getRandomString(length int) string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = str[rng.Intn(62)]
	}
	return string(b)
}
