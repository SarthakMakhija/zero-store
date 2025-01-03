package filter

import (
	"bufio"
	"bytes"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/bits-and-blooms/bloom/v3"
)

// BloomFilter is a wrapper over filter.BloomFilter.
type BloomFilter struct {
	filter *bloom.BloomFilter
}

// newBloomFilter creates a new instance of BloomFilter.
func newBloomFilter(filter *bloom.BloomFilter) BloomFilter {
	return BloomFilter{
		filter: filter,
	}
}

// DecodeToBloomFilter creates a new instance of BloomFilter from the given byte slice.
func DecodeToBloomFilter(data []byte) (BloomFilter, error) {
	filter := &bloom.BloomFilter{}
	_, err := filter.ReadFrom(bytes.NewReader(data))
	if err != nil {
		return BloomFilter{}, err
	}
	return newBloomFilter(filter), nil
}

// Encode returns the bloom filter bits as byte slice.
func (filter BloomFilter) Encode() ([]byte, error) {
	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)

	if _, err := filter.filter.WriteTo(writer); err != nil {
		return nil, err
	}
	// Flush the writer to ensure all data is written to the buffer.
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// MayContain returns true if the given key may be present in the bloom filter, false otherwise.
func (filter BloomFilter) MayContain(key kv.Key) bool {
	return filter.filter.Test(key.RawBytes())
}

// add adds the given key in the bloom filter.
func (filter BloomFilter) add(key kv.Key) {
	filter.filter.Add(key.RawBytes())
}
