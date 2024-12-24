package key_cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/coocood/freecache"
	"github.com/huandu/skiplist"
	"log/slog"
	"math"
	"sync"
)

type KeyCache struct {
	rawKeyCache     *rawKeyCache
	keyIdCache      *keyIdCache
	lock            sync.RWMutex
	evictionChannel chan keyId
	stopChannel     chan struct{}
}

func NewKeyCache(options KeyCacheOptions) *KeyCache {
	evictionChannel := make(chan keyId, 1024)
	cache := &KeyCache{
		rawKeyCache: newRawKeyCache(options, func(keyIdAsBytes []byte) {
			evictionChannel <- decodeKeyIdFrom(keyIdAsBytes)
		}),
		keyIdCache:      newKeyIdCache(),
		evictionChannel: evictionChannel,
		stopChannel:     make(chan struct{}),
	}
	go cache.spawnKeyEvictionHandler()
	return cache
}

func (cache *KeyCache) Set(key kv.Key, timestamp uint64, value kv.Value) {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	id, err := cache.rawKeyCache.add(key)
	if err != nil {
		slog.Warn("failed to add key in cache", "err", err)
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

func (cache *KeyCache) Stop() {
	close(cache.stopChannel)
}

func (cache *KeyCache) spawnKeyEvictionHandler() {
	for {
		select {
		case evictedKeyId := <-cache.evictionChannel:
			cache.lock.Lock()
			cache.keyIdCache.removeAllOccurrencesOf(evictedKeyId)
			cache.lock.Unlock()
		case <-cache.stopChannel:
			return
		}
	}
}

///////////////////////rawKeyCache///////////////////////

// TODO: remove locks from freecache or revisit locking strategy in keycache.
// TODO: This will also depend on the fact that zero-store might store a bunch of keys in the rawKeyCache in one go.
type rawKeyCache struct {
	cache       *freecache.Cache
	idGenerator *keyIdGenerator
	options     KeyCacheOptions
}

func newRawKeyCache(options KeyCacheOptions, evictionCallback func(keyIdAsBytes []byte)) *rawKeyCache {
	return &rawKeyCache{
		cache:       freecache.NewCacheEvictionCallback(int(options.sizeInBytes), evictionCallback),
		idGenerator: newKeyIdGenerator(),
		options:     options,
	}
}

func (cache *rawKeyCache) add(key kv.Key) (keyId, error) {
	cachedKeyId, ok := cache.getKeyId(key)
	if !ok {
		err := cache.cache.Set(key.RawBytes(), cache.idGenerator.nextIdAsBytes(), int(cache.options.entryTTL.Seconds()))
		if err != nil {
			slog.Warn("failed to add key in rawKeyCache", "err", err)
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
	cache.cache.Set(key, value)
}

func (cache *keyIdCache) get(key timestampedKeyId) (kv.Value, bool) {
	element := cache.cache.Find(key)
	if element == nil || element.Key().(timestampedKeyId).keyId != key.keyId {
		return kv.EmptyValue, false
	}
	return element.Value.(kv.Value), true
}

func (cache *keyIdCache) removeAllOccurrencesOf(id keyId) {
	element := cache.cache.Find(newTimestampedKeyId(id, math.MaxUint64))
	for element != nil && element.Key().(timestampedKeyId).keyId == id {
		next := element.Next()
		cache.cache.RemoveElement(element)

		element = next
	}
}
