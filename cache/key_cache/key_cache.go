package key_cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/coocood/freecache"
	"log/slog"
)

type KeyCache struct {
	rawKeyCache *freecache.Cache
	idGenerator *keyIdGenerator
	options     KeyCacheOptions
}

func NewKeyCache(options KeyCacheOptions) *KeyCache {
	return &KeyCache{
		rawKeyCache: freecache.NewCache(int(options.sizeInBytes)),
		idGenerator: newKeyIdGenerator(),
		options:     options,
	}
}

func (cache *KeyCache) Set(key kv.Key, value kv.Value) {
	_, ok := cache.getKeyId(key)
	if !ok {
		err := cache.rawKeyCache.Set(key.RawBytes(), cache.idGenerator.nextIdAsBytes(), int(cache.options.entryTTL.Seconds()))
		if err != nil {
			slog.Warn("failed to set key in cache", "err", err)
			return
		}
	}
	//TODO: handle exists
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
