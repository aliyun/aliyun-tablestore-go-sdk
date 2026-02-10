package common

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"sync"
	"time"
)

const (
	seed                 = "aliyun"
	prefix               = seed + "_v4"
	constant             = seed + "_v4_request"
	signingKeySignMethod = "HmacSHA256"
	product              = "ots"
)

// CredentialInf is interface for get AccessKeyID,AccessKeySecret,SecurityToken
type Credentials interface {
	GetAccessKeyID() string
	GetAccessKeySecret() string
	GetSecurityToken() string
}

// CredentialInfBuild is interface for get CredentialInf
type CredentialsProvider interface {
	GetCredentials() Credentials
}

type DefaultCredentials struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
}

func (defCre *DefaultCredentials) GetAccessKeyID() string {
	return defCre.AccessKeyID
}

func (defCre *DefaultCredentials) GetAccessKeySecret() string {
	return defCre.AccessKeySecret
}

func (defCre *DefaultCredentials) GetSecurityToken() string {
	return defCre.SecurityToken
}

type DefaultCredentialsProvider struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
}

func (defBuild *DefaultCredentialsProvider) GetCredentials() Credentials {
	return &DefaultCredentials{AccessKeyID: defBuild.AccessKeyID, AccessKeySecret: defBuild.AccessKeySecret, SecurityToken: defBuild.SecurityToken}
}

type CredentialsV4 interface {
	Credentials
	GetRegion() string
	GetSigningDate() string
	GetKeyDatePair() (string, string)
	GetSnapshot() *V4CredentialsSnapshot
}

type V4Credentials struct {
	AccessKeyID                  string
	AccessKeySecret              string
	V4SigningStsToken            string
	Region                       string
	SigningDate                  string
	V4SigningAccessKey           string
	autoUpdateV4SigningAccessKey bool
	mu                           sync.RWMutex
}

func (v4Cre *V4Credentials) GetAccessKeyID() string {
	return v4Cre.AccessKeyID
}

func (v4Cre *V4Credentials) GetAccessKeySecret() string {
	v4Cre.UpdateV4Signature()
	return v4Cre.V4SigningAccessKey
}

func (v4Cre *V4Credentials) GetSecurityToken() string {
	return v4Cre.V4SigningStsToken
}

func (v4Cre *V4Credentials) GetRegion() string {
	return v4Cre.Region
}

func (v4Cre *V4Credentials) GetSigningDate() string {
	return v4Cre.SigningDate
}

func (v4Cre *V4Credentials) GetCredentials() Credentials {
	return v4Cre
}

func (v4Cre *V4Credentials) GetKeyDatePair() (string, string) {
	v4Cre.mu.RLock()
	defer v4Cre.mu.RUnlock()
	return v4Cre.V4SigningAccessKey, v4Cre.SigningDate
}

func (v4Cre *V4Credentials) GetSnapshot() *V4CredentialsSnapshot {
	keySnapshot, dateSnapshot := v4Cre.GetKeyDatePair()
	return &V4CredentialsSnapshot{
		AccessKeyID:        v4Cre.GetAccessKeyID(),
		AccessKeySecret:    v4Cre.GetAccessKeySecret(),
		Region:             v4Cre.GetRegion(),
		SigningDate:        dateSnapshot,
		V4SigningAccessKey: keySnapshot,
	}
}

func (v4Cre *V4Credentials) UpdateV4Signature() {
	if v4Cre.autoUpdateV4SigningAccessKey {
		dateNow := GetFormattedDate()
		if dateNow != v4Cre.SigningDate {
			v4Cre.mu.Lock()
			defer v4Cre.mu.Unlock()
			if dateNow != v4Cre.SigningDate {
				v4Cre.SigningDate = dateNow
				v4Cre.V4SigningAccessKey = FinalSigningKeyString(v4Cre.AccessKeySecret, v4Cre.SigningDate, v4Cre.Region, product, signingKeySignMethod)
			}
		}
	}
}

type V4CredentialsSnapshot struct {
	AccessKeyID        string
	AccessKeySecret    string
	V4SigningStsToken  string
	Region             string
	SigningDate        string
	V4SigningAccessKey string
}

func (v4Cre *V4CredentialsSnapshot) GetAccessKeyID() string {
	return v4Cre.AccessKeyID
}

func (v4Cre *V4CredentialsSnapshot) GetAccessKeySecret() string {
	return v4Cre.V4SigningAccessKey
}

func (v4Cre *V4CredentialsSnapshot) GetSecurityToken() string {
	return v4Cre.V4SigningStsToken
}

func (v4Cre *V4CredentialsSnapshot) GetRegion() string {
	return v4Cre.Region
}

func (v4Cre *V4CredentialsSnapshot) GetSigningDate() string {
	return v4Cre.SigningDate
}

func (v4Cre *V4CredentialsSnapshot) GetCredentials() Credentials {
	return v4Cre
}

func (v4Cre *V4CredentialsSnapshot) GetKeyDatePair() (string, string) {
	return v4Cre.V4SigningAccessKey, v4Cre.SigningDate
}

func (v4Cre *V4CredentialsSnapshot) GetSnapshot() *V4CredentialsSnapshot {
	return v4Cre
}

func GetFormattedDate() string {
	now := time.Now().UTC()
	return now.Format("20060102")
}

func CreateByCredentials(credentials Credentials, region string) *V4Credentials {
	v4Cre := &V4Credentials{}
	v4Cre.AccessKeyID = credentials.GetAccessKeyID()
	v4Cre.AccessKeySecret = credentials.GetAccessKeySecret()
	v4Cre.V4SigningStsToken = credentials.GetSecurityToken()
	v4Cre.Region = region
	v4Cre.SigningDate = GetFormattedDate()
	v4Cre.V4SigningAccessKey = FinalSigningKeyString(v4Cre.AccessKeySecret, v4Cre.SigningDate, v4Cre.Region, product, signingKeySignMethod)
	v4Cre.autoUpdateV4SigningAccessKey = true
	return v4Cre
}

func GetHmac(signMethod string, key []byte) hash.Hash {
	var hMac hash.Hash
	if signMethod == "HmacSHA1" {
		hMac = hmac.New(sha1.New, key)
	} else if signMethod == "HmacSHA256" {
		hMac = hmac.New(sha256.New, key)
	}
	return hMac
}

func FirstSigningKey(secret string, date string, signMethod string) []byte {
	hMac := GetHmac(signMethod, []byte(prefix+secret))
	hMac.Reset()
	hMac.Write([]byte(date))
	return hMac.Sum(nil)
}

func RegionSigningKey(secret string, date string, region string, signMethod string) []byte {
	firstSignkey := FirstSigningKey(secret, date, signMethod)
	hMac := GetHmac(signMethod, firstSignkey)
	hMac.Reset()
	hMac.Write([]byte(region))
	return hMac.Sum(nil)
}

func RegionSigningKeyString(secret string, date string, region string, signMethod string) string {
	return base64.StdEncoding.EncodeToString(RegionSigningKey(secret, date, region, signMethod))
}

func FinalSigningKey(secret string, date string, region string, productCode string, signMethod string) []byte {
	secondSignify := RegionSigningKey(secret, date, region, signMethod)
	hMac := GetHmac(signMethod, secondSignify)
	hMac.Reset()
	hMac.Write([]byte(productCode))
	thirdSigningKey := hMac.Sum(nil)
	hMac = GetHmac(signMethod, thirdSigningKey)
	hMac.Reset()
	hMac.Write([]byte(constant))
	return hMac.Sum(nil)
}

func FinalSigningKeyString(secret string, date string, region string, productCode string, signMethod string) string {
	return base64.StdEncoding.EncodeToString(FinalSigningKey(secret, date, region, productCode, signMethod))
}
