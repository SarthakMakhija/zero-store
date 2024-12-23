package key_cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/coocood/freecache"
	"github.com/huandu/skiplist"
	"log/slog"
	"sync"
)

type KeyCache struct {
	rawKeyCache *rawKeyCache
	keyIdCache  *keyIdCache
	lock        sync.RWMutex
}

func NewKeyCache(options KeyCacheOptions) *KeyCache {
	return &KeyCache{
		rawKeyCache: newRawKeyCache(options),
		keyIdCache:  newKeyIdCache(),
	}
}

func (cache *KeyCache) Set(key kv.Key, timestamp uint64, value kv.Value) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	id, err := cache.rawKeyCache.set(key)
	if err != nil {
		slog.Warn("failed to set key in cache", "err", err)
		return
	}
	cache.keyIdCache.set(newTimestampedKeyId(id, timestamp), value)
}

func (cache *KeyCache) Get(key kv.Key, timestamp uint64) (kv.Value, bool) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	cachedKeyId, ok := cache.rawKeyCache.getKeyId(key)
	if !ok {
		return kv.EmptyValue, false
	}
	value, ok := cache.keyIdCache.get(newTimestampedKeyId(cachedKeyId, timestamp))
	if !ok {
		return kv.EmptyValue, false
	}
	return value, true
}

///////////////////////rawKeyCache///////////////////////

type rawKeyCache struct {
	cache       *freecache.Cache
	idGenerator *keyIdGenerator
	options     KeyCacheOptions
}

func newRawKeyCache(options KeyCacheOptions) *rawKeyCache {
	return &rawKeyCache{
		cache:       freecache.NewCache(int(options.sizeInBytes)),
		idGenerator: newKeyIdGenerator(),
		options:     options,
	}
}

func (cache *rawKeyCache) set(key kv.Key) (keyId, error) {
	cachedKeyId, ok := cache.getKeyId(key)
	if !ok {
		err := cache.cache.Set(key.RawBytes(), cache.idGenerator.nextIdAsBytes(), int(cache.options.entryTTL.Seconds()))
		if err != nil {
			slog.Warn("failed to set key in rawKeyCache", "err", err)
			return 0, err
		}
		return cache.idGenerator.id, nil
	}
	return cachedKeyId, nil
}

func (cache *rawKeyCache) getKeyId(key kv.Key) (keyId, bool) {
	var id keyId
	err := cache.cache.GetFn(key.RawBytes(), func(keyIdBuffer []byte) error {
		id = decodeKeyIdFrom(keyIdBuffer)
		return nil
	})
	if err != nil {
		return 0, false
	}
	return id, true
}

///////////////////////keyIdCache///////////////////////

type keyIdCache struct {
	cache *skiplist.SkipList
}

func newKeyIdCache() *keyIdCache {
	return &keyIdCache{
		cache: skiplist.New(skiplist.GreaterThanFunc(func(key, other interface{}) int {
			return compareKeysWithDescendingTimestamp(key, other)
		})),
	}
}

func (cache *keyIdCache) set(key timestampedKeyId, value kv.Value) {
	cache.cache.Set(key, value.EncodedBytes())
}

func (cache *keyIdCache) get(key timestampedKeyId) (kv.Value, bool) {
	element := cache.cache.Find(key)
	return kv.DecodeValueFrom(element.Value.([]byte)), true
}
