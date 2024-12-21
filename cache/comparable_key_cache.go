package cache

import (
	"github.com/maypok86/otter"
)

type ComparableKeyCache[K comparable, V any] struct {
	cache otter.Cache[K, V]
}

func NewComparableKeyCache[K comparable, V any](options ComparableKeyCacheOptions[K, V]) (ComparableKeyCache[K, V], error) {
	cache, err := otter.MustBuilder[K, V](int(options.sizeInBytes)).
		Cost(func(key K, value V) uint32 {
			return options.costFn(key, value)
		}).
		WithTTL(options.entryTTL).
		Build()

	if err != nil {
		return ComparableKeyCache[K, V]{}, err
	}
	return ComparableKeyCache[K, V]{
		cache: cache,
	}, nil
}

func (cache ComparableKeyCache[K, V]) Set(key K, value V) bool {
	return cache.cache.Set(key, value)
}

func (cache ComparableKeyCache[K, V]) Get(key K) (V, bool) {
	return cache.cache.Get(key)
}
