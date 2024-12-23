package key_cache

import "time"

type KeyCacheOptions struct {
	sizeInBytes uint
	entryTTL    time.Duration
}

func NewKeyCacheOptions(sizeInBytes uint, entryTTL time.Duration) KeyCacheOptions {
	return KeyCacheOptions{
		sizeInBytes: sizeInBytes,
		entryTTL:    entryTTL,
	}
}
