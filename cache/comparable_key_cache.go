package cache

import (
	"github.com/maypok86/otter"
)

type comparableKeyCache[K comparable, V any] struct {
	cache otter.Cache[K, V]
}

func newComparableKeyCache[K comparable, V any](options ComparableKeyCacheOptions[K, V]) (comparableKeyCache[K, V], error) {
	cache, err := otter.MustBuilder[K, V](int(options.sizeInBytes)).
		Cost(func(key K, value V) uint32 {
			return options.costFn(key, value)
		}).
		WithTTL(options.entryTTL).
		Build()

	if err != nil {
		return comparableKeyCache[K, V]{}, err
	}
	return comparableKeyCache[K, V]{
		cache: cache,
	}, nil
}

func (cache comparableKeyCache[K, V]) Set(key K, value V) bool {
	return cache.cache.Set(key, value)
}

func (cache comparableKeyCache[K, V]) Get(key K) (V, bool) {
	return cache.cache.Get(key)
}
