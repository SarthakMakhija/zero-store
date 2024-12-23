package key_cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKeyIdGeneratorNextId(t *testing.T) {
	idGenerator := newKeyIdGenerator()
	buffer := idGenerator.nextIdAsBytes()

	assert.Equal(t, keyId(1), decodeKeyIdFrom(buffer))
}

func TestKeyIdGeneratorNextIdAFewTimes(t *testing.T) {
	idGenerator := newKeyIdGenerator()
	idGenerator.nextIdAsBytes()
	idGenerator.nextIdAsBytes()
	buffer := idGenerator.nextIdAsBytes()

	assert.Equal(t, keyId(3), decodeKeyIdFrom(buffer))
}
