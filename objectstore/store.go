package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
)

var (
	errObjectExists = errors.New("object already exists")
)

type Store struct {
	rootPath   string
	definition StoreDefinition
}

func NewStore(rootPath string, definition StoreDefinition) Store {
	return Store{
		rootPath:   rootPath,
		definition: definition,
	}
}

func (store Store) Set(pathSuffix string, buffer []byte) error {
	objectPath := store.objectPath(pathSuffix)
	exists, err := store.definition.Exists(context.Background(), store.objectPath(pathSuffix))
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: %s", errObjectExists, objectPath)
	}
	return store.definition.Upload(context.Background(), objectPath, bytes.NewReader(buffer))
}

func (store Store) Get(pathSuffix string) ([]byte, error) {
	reader, err := store.definition.Get(context.Background(), store.objectPath(pathSuffix))
	if err != nil {
		return nil, err
	}
	buffer, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (store Store) GetRange(pathSuffix string, startOffset int64, length int64) ([]byte, error) {
	reader, err := store.definition.GetRange(context.Background(), store.objectPath(pathSuffix), startOffset, length)
	if err != nil {
		return nil, err
	}
	buffer, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (store Store) Close() {
	_ = store.definition.Close()
}

func (store Store) objectPath(pathSuffix string) string {
	return path.Join(store.rootPath, pathSuffix)
}
