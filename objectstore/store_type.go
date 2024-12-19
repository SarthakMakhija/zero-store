package objectstore

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

func (storeType StoreType) GetStore(rootPath string) (Store, error) {
	switch storeType {
	case FileSystemStore:
		storeDefinition, err := NewFileSystemStoreDefinition(rootPath)
		if err != nil {
			return Store{}, err
		}
		return NewStore(rootPath, storeDefinition), nil
	default:
		panic("unknown store type")
	}
}
