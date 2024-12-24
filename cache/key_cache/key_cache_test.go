package key_cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRawKeyCacheSetKey(t *testing.T) {
	rawKeyCache := newRawKeyCache(NewKeyCacheOptions(512*1024, time.Second*10), nil)
	id, err := rawKeyCache.add(kv.NewStringKey("consensus"))

	assert.NoError(t, err)
	assert.Equal(t, keyId(1), id)
}

func TestRawKeyCacheSetKeyAndGetByKey(t *testing.T) {
	rawKeyCache := newRawKeyCache(NewKeyCacheOptions(512*1024, time.Second*10), nil)
	id, err := rawKeyCache.add(kv.NewStringKey("consensus"))

	assert.NoError(t, err)
	assert.Equal(t, keyId(1), id)

	cachedKeyId, ok := rawKeyCache.getKeyId(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, keyId(1), cachedKeyId)
}

func TestRawKeyCacheSetKeyAndAttemptToGetKeyIdForANonExistingKey(t *testing.T) {
	rawKeyCache := newRawKeyCache(NewKeyCacheOptions(512*1024, time.Second*10), nil)
	id, err := rawKeyCache.add(kv.NewStringKey("consensus"))

	assert.NoError(t, err)
	assert.Equal(t, keyId(1), id)

	cachedKeyId, ok := rawKeyCache.getKeyId(kv.NewStringKey("non-existing"))
	assert.False(t, ok)
	assert.Equal(t, keyId(0), cachedKeyId)
}

func TestKeyIdCacheWithASingleTimestampOfAKeyId(t *testing.T) {
	cache := newKeyIdCache()
	cache.set(newTimestampedKeyId(keyId(10), 20), kv.NewStringValue("raft"))

	value, ok := cache.get(newTimestampedKeyId(keyId(10), 20))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("raft"), value)
}

func TestKeyIdCacheWithAMultipleTimestampsOfAKeyId(t *testing.T) {
	cache := newKeyIdCache()
	cache.set(newTimestampedKeyId(keyId(10), 20), kv.NewStringValue("raft"))
	cache.set(newTimestampedKeyId(keyId(10), 30), kv.NewStringValue("paxos"))
	cache.set(newTimestampedKeyId(keyId(10), 40), kv.NewStringValue("VSR"))

	value, ok := cache.get(newTimestampedKeyId(keyId(10), 40))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("VSR"), value)
}

func TestKeyIdCacheWithAMultipleTimestampsOfAKeyIdAndGetValueByKeyIdSuchThatTimestampIsLessThanTheTimestampOfKeyIdInTheCache(t *testing.T) {
	cache := newKeyIdCache()
	cache.set(newTimestampedKeyId(keyId(10), 20), kv.NewStringValue("raft"))
	cache.set(newTimestampedKeyId(keyId(10), 30), kv.NewStringValue("paxos"))
	cache.set(newTimestampedKeyId(keyId(10), 40), kv.NewStringValue("VSR"))

	value, ok := cache.get(newTimestampedKeyId(keyId(10), 45))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("VSR"), value)
}

func TestKeyIdCacheRemoveAllOccurrencesOfAKeyId(t *testing.T) {
	cache := newKeyIdCache()
	cache.set(newTimestampedKeyId(keyId(10), 20), kv.NewStringValue("raft"))
	cache.set(newTimestampedKeyId(keyId(10), 30), kv.NewStringValue("paxos"))
	cache.set(newTimestampedKeyId(keyId(10), 40), kv.NewStringValue("VSR"))

	cache.removeAllOccurrencesOf(keyId(10))

	value, ok := cache.get(newTimestampedKeyId(keyId(10), 50))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestKeyIdCacheRemoveAllOccurrencesOfTheOnlySpecifiedKeyId1(t *testing.T) {
	cache := newKeyIdCache()
	cache.set(newTimestampedKeyId(keyId(10), 20), kv.NewStringValue("raft"))
	cache.set(newTimestampedKeyId(keyId(10), 30), kv.NewStringValue("paxos"))
	cache.set(newTimestampedKeyId(keyId(10), 40), kv.NewStringValue("VSR"))
	cache.set(newTimestampedKeyId(keyId(100), 15), kv.NewStringValue("etcd"))

	cache.removeAllOccurrencesOf(keyId(10))

	value, ok := cache.get(newTimestampedKeyId(keyId(100), 18))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("etcd"), value)
}

func TestKeyIdCacheRemoveAllOccurrencesOfTheOnlySpecifiedKeyId2(t *testing.T) {
	cache := newKeyIdCache()
	cache.set(newTimestampedKeyId(keyId(10), 20), kv.NewStringValue("raft"))
	cache.set(newTimestampedKeyId(keyId(10), 30), kv.NewStringValue("paxos"))
	cache.set(newTimestampedKeyId(keyId(10), 40), kv.NewStringValue("VSR"))
	cache.set(newTimestampedKeyId(keyId(100), 15), kv.NewStringValue("etcd"))
	cache.set(newTimestampedKeyId(keyId(100), 18), kv.NewStringValue("foundationDb"))

	cache.removeAllOccurrencesOf(keyId(10))

	value, ok := cache.get(newTimestampedKeyId(keyId(100), 20))
	assert.True(t, ok)
	assert.Equal(t, kv.NewStringValue("foundationDb"), value)
}

func TestKeyCacheSetKeyAndGetValueByKey(t *testing.T) {
	timestamp := uint64(1)
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), timestamp, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), timestamp, kv.NewStringValue("etcd"))

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), timestamp)
	assert.True(t, ok)
	assert.Equal(t, "raft", value.String())

	value, ok = keyCache.Get(kv.NewStringKey("distributed"), timestamp)
	assert.True(t, ok)
	assert.Equal(t, "etcd", value.String())
}

func TestKeyCacheSetKeyAndAttemptToGetValueForANonExistingKey(t *testing.T) {
	timestamp := uint64(1)
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), timestamp, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), timestamp, kv.NewStringValue("etcd"))

	value, ok := keyCache.Get(kv.NewStringKey("non-existing"), timestamp)
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestKeyCacheSetKeyAndGetValueByKeySuchThatTimestampIsLessThanTheTimestampOfKeyInTheCache(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), 2, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), 3, kv.NewStringValue("etcd"))

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), 3)
	assert.True(t, ok)
	assert.Equal(t, "raft", value.String())

	value, ok = keyCache.Get(kv.NewStringKey("distributed"), 4)
	assert.True(t, ok)
	assert.Equal(t, "etcd", value.String())
}

func TestKeyCacheSetKeyAndGetValueByKeySuchThatTimestampDoesNotMatch(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), 2, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), 3, kv.NewStringValue("etcd"))

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), 1)
	assert.False(t, ok)
	assert.Equal(t, "", value.String())

	value, ok = keyCache.Get(kv.NewStringKey("distributed"), 2)
	assert.False(t, ok)
	assert.Equal(t, "", value.String())
}

func TestKeyCacheSetKeyAndGetValueByKeySuchThatTimestampIsLessThanTheTimestampOfKeyInTheCacheAndAKeyHasMultipleTimestampsInCache(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), 2, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("consensus"), 3, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), 3, kv.NewStringValue("etcd"))
	keyCache.Set(kv.NewStringKey("distributed"), 4, kv.NewStringValue("foundationDb"))

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), 4)
	assert.True(t, ok)
	assert.Equal(t, "raft", value.String())

	value, ok = keyCache.Get(kv.NewStringKey("distributed"), 4)
	assert.True(t, ok)
	assert.Equal(t, "foundationDb", value.String())
}

func TestKeyCacheSimulateEviction(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), 2, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), 3, kv.NewStringValue("etcd"))

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), 4)
	assert.True(t, ok)
	assert.Equal(t, "raft", value.String())

	id, ok := keyCache.rawKeyCache.getKeyId(kv.NewStringKey("consensus"))
	assert.True(t, ok)

	keyCache.evictionChannel <- id
	time.Sleep(time.Millisecond * 4)

	value, ok = keyCache.Get(kv.NewStringKey("consensus"), 4)
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestKeyCacheSimulateEvictionOfAKeyWithMultipleTimestamps(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), 2, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("consensus"), 4, kv.NewStringValue("paxos"))
	keyCache.Set(kv.NewStringKey("distributed"), 5, kv.NewStringValue("etcd"))

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), 4)
	assert.True(t, ok)
	assert.Equal(t, "paxos", value.String())

	id, ok := keyCache.rawKeyCache.getKeyId(kv.NewStringKey("consensus"))
	assert.True(t, ok)

	keyCache.evictionChannel <- id
	time.Sleep(time.Millisecond * 4)

	value, ok = keyCache.Get(kv.NewStringKey("consensus"), 4)
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestKeyCacheWithEvictionOfAKey(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*1))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), 2, kv.NewStringValue("raft"))

	id, ok := keyCache.rawKeyCache.getKeyId(kv.NewStringKey("consensus"))
	assert.True(t, ok)

	time.Sleep(time.Second)

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), 4)
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)

	time.Sleep(20 * time.Millisecond)

	value, ok = keyCache.keyIdCache.get(newTimestampedKeyId(id, 2))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestKeyCacheWithEvictionOfACoupleOfKeys(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*1))
	defer keyCache.Stop()

	keyCache.Set(kv.NewStringKey("consensus"), 2, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), 3, kv.NewStringValue("etcd"))

	idConsensus, ok := keyCache.rawKeyCache.getKeyId(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	idDistributed, ok := keyCache.rawKeyCache.getKeyId(kv.NewStringKey("distributed"))
	assert.True(t, ok)

	time.Sleep(time.Second)

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), 4)
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)

	value, ok = keyCache.Get(kv.NewStringKey("distributed"), 4)
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)

	time.Sleep(20 * time.Millisecond)

	value, ok = keyCache.keyIdCache.get(newTimestampedKeyId(idConsensus, 4))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)

	value, ok = keyCache.keyIdCache.get(newTimestampedKeyId(idDistributed, 4))
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}
