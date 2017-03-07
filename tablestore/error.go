package tablestore

import (
	"errors"
)

var (
	errMissMustHeader = func(header string) error {
		return errors.New("[tablestore] miss must header: " + header)
	}
	errTableNameTooLong = func(name string) error {
		return errors.New("[tablestore] table name: \"" + name + "\" too long")
	}
	errPrimaryKeyTooLong = func(key string) error {
		return errors.New("[tablestore] primary: \"" + key + "\" too long")
	}
	otsErrorInfo = func(code, msg string) error {
		return errors.New("[tablestore] error code: \"" + code + "\" message: \"" + msg + "\"")
	}

	errInvalidPartitionType = errors.New("[tablestore] invalid partition key")
	errMissPrimaryKey = errors.New("[tablestore] missing primary key")
	errPrimaryKeyTooMuch = errors.New("[tablestore] primary key too much")
	errMultiDeleteRowsTooMuch = errors.New("[tablestore] multi delete rows too much")
	errCreateTableNoPrimaryKey = errors.New("[tablestore] create table no primary key")
	errUnexpectIoEnd = errors.New("[tablestore] unexpect io end")
	errTag = errors.New("[tablestore] unexpect tag")
	errNoChecksum = errors.New("[tablestore] expect checksum")
	errChecksum = errors.New("[tablestore] checksum failed")
	errInvalidInput = errors.New("[tablestore] checksum failed")
)

type OtsError struct {
	IoError error
	Error   error
	retry   bool
}

type otsException struct {
	httpCode int
	errCode  string
	errMsg   string
}

func (e *otsException) equal(httpCode int, errCode, errMsg string) bool {
	return e.httpCode == httpCode && e.errCode == errCode && e.errMsg == errMsg
}

// 400 OTSStorageTxnLockKeyFail	   "Transaction timeout because cannot acquire exclusive lock."
// 400 OTSStoragePartitionNotReady "The partition has not been loaded."
// 500 OTSStorageTimeout           "Operation timeout."
// 500 OTSStorageServerBusy		   "Service is busy."
// 500 OTSStoragePartitionReadOnly "Partition is being split or in maintenance, please try again later."

var (
	retryExceptions = []otsException{
		otsException{
			httpCode: 400,
			errCode:  "OTSStorageTxnLockKeyFail",
			errMsg:   "Transaction timeout because cannot acquire exclusive lock.",
		},

		otsException{
			httpCode: 400,
			errCode:  "OTSStoragePartitionNotReady",
			errMsg:   "The partition has not been loaded.",
		},

		otsException{
			httpCode: 500,
			errCode:  "OTSStorageTimeout",
			errMsg:   "Operation timeout.",
		},

		otsException{
			httpCode: 500,
			errCode:  "OTSStorageServerBusy",
			errMsg:   "Service is busy.",
		},

		otsException{
			httpCode: 500,
			errCode:  "OTSStoragePartitionReadOnly",
			errMsg:   "Partition is being split or in maintenance, please try again later.",
		},
	}
)
