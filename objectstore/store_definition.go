package objectstore

import (
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

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
