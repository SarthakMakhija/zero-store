package cache

import "time"

type BloomFilterCacheOptions struct {
	sizeInBytes uint
	entryTTL    time.Duration
}

func NewBloomFilterCacheOptions(sizeInBytes uint, entryTTL time.Duration) BloomFilterCacheOptions {
	return BloomFilterCacheOptions{
		sizeInBytes: sizeInBytes,
		entryTTL:    entryTTL,
	}
}
