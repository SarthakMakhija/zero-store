package key_cache

import (
	"encoding/binary"
	"unsafe"
)

const keyIdSize = unsafe.Sizeof(uint64(0))

type keyId uint64

type keyIdGenerator struct {
	id keyId
}

func newKeyIdGenerator() *keyIdGenerator {
	return &keyIdGenerator{id: 0}
}

func (generator *keyIdGenerator) nextIdAsBytes() []byte {
	generator.id = generator.id + 1
	return generator.id.encodedBytes()
}

func decodeKeyIdFrom(buffer []byte) keyId {
	return keyId(binary.LittleEndian.Uint64(buffer[:]))
}

func (id keyId) encodedBytes() []byte {
	buffer := make([]byte, keyIdSize)
	binary.LittleEndian.PutUint64(buffer, uint64(id))

	return buffer
}
