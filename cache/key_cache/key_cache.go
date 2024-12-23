package key_cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/coocood/freecache"
	"github.com/huandu/skiplist"
	"log/slog"
)

type KeyCache struct {
	rawKeyCache *freecache.Cache
	keyIdCache  *skiplist.SkipList
	idGenerator *keyIdGenerator
	options     KeyCacheOptions
}

func NewKeyCache(options KeyCacheOptions) *KeyCache {
	return &KeyCache{
		rawKeyCache: freecache.NewCache(int(options.sizeInBytes)),
		keyIdCache:  skiplist.New(skiplist.Uint64),
		idGenerator: newKeyIdGenerator(),
		options:     options,
	}
}

func (cache *KeyCache) Set(key kv.Key, value kv.Value) {
	cachedKeyId, ok := cache.getKeyId(key)
	if !ok {
		err := cache.rawKeyCache.Set(key.RawBytes(), cache.idGenerator.nextIdAsBytes(), int(cache.options.entryTTL.Seconds()))
		if err != nil {
			slog.Warn("failed to set key in cache", "err", err)
			return
		}
		cache.keyIdCache.Set(cache.idGenerator.id, value.EncodedBytes())
		return
	}
	cache.keyIdCache.Set(cachedKeyId, value.EncodedBytes())
}

func (cache *KeyCache) Get(key kv.Key) (kv.Value, bool) {
	cachedKeyId, ok := cache.getKeyId(key)
	if !ok {
		return kv.EmptyValue, false
	}
	elem := cache.keyIdCache.Get(cachedKeyId)
	return kv.DecodeValueFrom(elem.Value.([]byte)), true
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
