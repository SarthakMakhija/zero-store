package cache

import "time"

type ComparableKeyCacheOptions[K comparable, V any] struct {
	sizeInBytes uint
	entryTTL    time.Duration
	costFn      func(key K, value V) uint32
}

func NewComparableKeyCacheOptions[K comparable, V any](sizeInBytes uint, entryTTL time.Duration, costFn func(key K, value V) uint32) ComparableKeyCacheOptions[K, V] {
	return ComparableKeyCacheOptions[K, V]{
		sizeInBytes: sizeInBytes,
		entryTTL:    entryTTL,
		costFn:      costFn,
	}
}