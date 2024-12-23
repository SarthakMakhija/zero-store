package key_cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestKeyCacheSetKeyAndGetKeyId(t *testing.T) {
	timestamp := uint64(1)
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	keyCache.Set(kv.NewStringKey("consensus"), timestamp, kv.NewStringValue("raft"))

	cachedKeyId, ok := keyCache.getKeyId(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, keyId(1), cachedKeyId)
}

func TestKeyCacheSetKeyAndAttemptToGetKeyIdForANonExistingKey(t *testing.T) {
	timestamp := uint64(1)
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	keyCache.Set(kv.NewStringKey("consensus"), timestamp, kv.NewStringValue("raft"))

	cachedKeyId, ok := keyCache.getKeyId(kv.NewStringKey("non-existing"))
	assert.False(t, ok)
	assert.Equal(t, keyId(0), cachedKeyId)
}

func TestKeyCacheSetKeyAndGetValueByKey(t *testing.T) {
	timestamp := uint64(1)
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))

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
	keyCache.Set(kv.NewStringKey("consensus"), timestamp, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), timestamp, kv.NewStringValue("etcd"))

	value, ok := keyCache.Get(kv.NewStringKey("non-existing"), timestamp)
	assert.False(t, ok)
	assert.Equal(t, kv.EmptyValue, value)
}

func TestKeyCacheSetKeyAndGetValueByKeySuchThatTimestampIsLessThanTheTimestampOfKeyInTheCache(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	keyCache.Set(kv.NewStringKey("consensus"), 2, kv.NewStringValue("raft"))
	keyCache.Set(kv.NewStringKey("distributed"), 3, kv.NewStringValue("etcd"))

	value, ok := keyCache.Get(kv.NewStringKey("consensus"), 3)
	assert.True(t, ok)
	assert.Equal(t, "raft", value.String())

	value, ok = keyCache.Get(kv.NewStringKey("distributed"), 4)
	assert.True(t, ok)
	assert.Equal(t, "etcd", value.String())
}

func TestKeyCacheSetKeyAndGetValueByKeySuchThatTimestampIsLessThanTheTimestampOfKeyInTheCacheAndAKeyHasMultipleVersions(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
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
