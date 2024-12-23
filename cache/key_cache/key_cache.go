package key_cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/coocood/freecache"
	"github.com/huandu/skiplist"
	"log/slog"
	"sync"
)

type KeyCache struct {
	rawKeyCache *freecache.Cache
	keyIdCache  *skiplist.SkipList
	idGenerator *keyIdGenerator
	options     KeyCacheOptions
	lock        sync.RWMutex
}

func NewKeyCache(options KeyCacheOptions) *KeyCache {
	return &KeyCache{
		rawKeyCache: freecache.NewCache(int(options.sizeInBytes)),
		keyIdCache: skiplist.New(skiplist.GreaterThanFunc(func(key, other interface{}) int {
			return compareKeysWithDescendingTimestamp(key, other)
		})),
		idGenerator: newKeyIdGenerator(),
		options:     options,
	}
}

func (cache *KeyCache) Set(key kv.Key, timestamp uint64, value kv.Value) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	cachedKeyId, ok := cache.getKeyId(key)
	if !ok {
		err := cache.rawKeyCache.Set(key.RawBytes(), cache.idGenerator.nextIdAsBytes(), int(cache.options.entryTTL.Seconds()))
		if err != nil {
			slog.Warn("failed to set key in cache", "err", err)
			return
		}
		cache.keyIdCache.Set(newTimestampedKeyId(cache.idGenerator.id, timestamp), value.EncodedBytes())
		return
	}
	cache.keyIdCache.Set(newTimestampedKeyId(cachedKeyId, timestamp), value.EncodedBytes())
}

func (cache *KeyCache) Get(key kv.Key, timestamp uint64) (kv.Value, bool) {
	cache.lock.RLock()
	defer cache.lock.RUnlock()

	cachedKeyId, ok := cache.getKeyId(key)
	if !ok {
		return kv.EmptyValue, false
	}
	element := cache.keyIdCache.Find(newTimestampedKeyId(cachedKeyId, timestamp))
	return kv.DecodeValueFrom(element.Value.([]byte)), true
}

func (cache *KeyCache) getKeyId(key kv.Key) (keyId, bool) {
	var id keyId
	err := cache.rawKeyCache.GetFn(key.RawBytes(), func(keyIdBuffer []byte) error {
		id = decodeKeyIdFrom(keyIdBuffer)
		return nil
	})
	if err != nil {
		return 0, false
	}
	return id, true
}
