package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdataFailed      = errors.New("index update failed")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("data directory maybe corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed max batch number")
	ErrMergeIsProcess         = errors.New("merge is in process, try again later")
	ErrDatabaseIsUsing        = errors.New("database directory is using, try again later")
	ErrMergeRatioUnreached    = errors.New("merge ratio is not reached")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough space for merge")
)
