package common

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/testConfig"
	. "gopkg.in/check.v1"
	"log"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

type CredentialsSuite struct{}

var _ = Suite(&CredentialsSuite{})

var (
	accessKeySecret = "credentials_test_secret"
	region          = "cn-hangzhou"
	signingDate     = "20240808"

	regionSigningKeyString = "sU/G/Zj/gXQC04NFJXCbt8q2gs2L6GPqVoxsEWqs1M0="
	finalSigningKeyString  = "RNN1/s8/m45SdD+EzXlvpyGxc4Enu0RiODs1Lg+xFpo="
)

func (s *CredentialsSuite) TestRegionSigningKeyString(c *C) {
	log.Println("TestRegionSigningKeyString started")

	curRegionSigningKeyString := RegionSigningKeyString(accessKeySecret, signingDate, region, signingKeySignMethod)
	c.Assert(curRegionSigningKeyString, Equals, regionSigningKeyString)

	log.Println("TestRegionSigningKeyString finished")
}

func (s *CredentialsSuite) TestFinalSigningKeyString(c *C) {
	log.Println("TestFinalSigningKeyString started")

	curFinalSigningKeyString := FinalSigningKeyString(accessKeySecret, signingDate, region, product, signingKeySignMethod)
	c.Assert(curFinalSigningKeyString, Equals, finalSigningKeyString)

	log.Println("TestFinalSigningKeyString finished")
}

func (s *CredentialsSuite) TestUpdateV4Signature(c *C) {
	log.Println("TestUpdateV4Signature started")

	provider := &DefaultCredentialsProvider{AccessKeyID: testConfig.OtsAccessId, AccessKeySecret: testConfig.OtsAccessKey}
	v4Credentials := CreateByCredentials(provider.GetCredentials(), testConfig.Region)
	v4Credentials.SigningDate = signingDate
	v4Credentials.UpdateV4Signature()
	c.Assert(v4Credentials.SigningDate, Equals, GetFormattedDate())

	log.Println("TestUpdateV4Signature finished")
}
