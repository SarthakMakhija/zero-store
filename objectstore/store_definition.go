package objectstore

import (
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

type StoreType int

const (
	FileSystemStore StoreType = 1
)

func (storeType StoreType) IsValid() bool {
	switch storeType {
	case FileSystemStore:
		return true
	default:
		return false
	}
}

type StoreDefinition interface {
	objstore.Bucket
}

type FileSystemStoreDefinition struct {
	StoreDefinition
}

func NewFileSystemStoreDefinition(rootDirectory string) (*FileSystemStoreDefinition, error) {
	bucket, err := filesystem.NewBucket(rootDirectory)
	if err != nil {
		return nil, err
	}
	return &FileSystemStoreDefinition{
		StoreDefinition: bucket,
	}, nil
}
