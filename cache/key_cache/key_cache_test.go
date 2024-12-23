package key_cache

import (
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestKeyCacheSetRawKeyAndGetKeyId(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	keyCache.Set(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	cachedKeyId, ok := keyCache.getKeyId(kv.NewStringKey("consensus"))
	assert.True(t, ok)
	assert.Equal(t, keyId(1), cachedKeyId)
}

func TestKeyCacheSetRawKeyAndAttemptToGetKeyIdForANonExistingKey(t *testing.T) {
	keyCache := NewKeyCache(NewKeyCacheOptions(512*1024, time.Second*10))
	keyCache.Set(kv.NewStringKey("consensus"), kv.NewStringValue("raft"))

	cachedKeyId, ok := keyCache.getKeyId(kv.NewStringKey("non-existing"))
	assert.False(t, ok)
	assert.Equal(t, keyId(0), cachedKeyId)
}
