package block

import (
	"bytes"
	"encoding/binary"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/klauspost/compress/s2"
)

// Meta represents a block metadata including the starting (/first), ending (/last) key and the starting offset
// of a block.
type Meta struct {
	BlockBeginOffset uint32
	StartingKey      kv.Key
	EndingKey        kv.Key
}

// MetaList is a collection of metadata about multiple blocks.
type MetaList struct {
	list              []Meta
	enableCompression bool
}

// NewBlockMetaList creates a new instance of MetaList.
func NewBlockMetaList(enableCompression bool) *MetaList {
	return &MetaList{
		enableCompression: enableCompression,
	}
}

// Add adds the block meta to the list.
func (metaList *MetaList) Add(meta Meta) {
	metaList.list = append(metaList.list, meta)
}

// Encode encodes the meta-list.
// Encoding includes:
/*
  ---------------------------------------------------------------------------------------------------------------
 | 4 bytes for the number of blocks | 4 bytes for block begin-offset | Encoded starting key | Encoded ending key |
  ---------------------------------------------------------------------------------------------------------------
                                    <-------------------------------------for each block------------------------>
*/
func (metaList *MetaList) Encode() []byte {
	numberOfBlocks := make([]byte, Uint32Size)
	binary.LittleEndian.PutUint32(numberOfBlocks, uint32(len(metaList.list)))

	resultingBuffer := new(bytes.Buffer)
	resultingBuffer.Write(numberOfBlocks)

	for _, blockMeta := range metaList.list {
		buffer := make(
			[]byte,
			Uint32Size+
				ReservedKeySize+
				blockMeta.StartingKey.EncodedSizeInBytes()+
				ReservedKeySize+
				blockMeta.EndingKey.EncodedSizeInBytes(),
		)

		binary.LittleEndian.PutUint32(buffer[:], blockMeta.BlockBeginOffset)

		binary.LittleEndian.PutUint16(buffer[Uint32Size:], uint16(blockMeta.StartingKey.EncodedSizeInBytes()))
		copy(buffer[Uint32Size+ReservedKeySize:], blockMeta.StartingKey.EncodedBytes())

		binary.LittleEndian.PutUint16(
			buffer[Uint32Size+ReservedKeySize+blockMeta.StartingKey.EncodedSizeInBytes():],
			uint16(blockMeta.EndingKey.EncodedSizeInBytes()),
		)
		copy(
			buffer[Uint32Size+ReservedKeySize+blockMeta.StartingKey.EncodedSizeInBytes()+ReservedKeySize:],
			blockMeta.EndingKey.EncodedBytes(),
		)
		resultingBuffer.Write(buffer)
	}
	if metaList.enableCompression {
		return s2.Encode(nil, resultingBuffer.Bytes())
	}
	return resultingBuffer.Bytes()
}

// GetAt returns the meta at the given index.
func (metaList *MetaList) GetAt(index int) (Meta, bool) {
	if index < len(metaList.list) {
		return metaList.list[index], true
	}
	return Meta{}, false
}

// Length returns the length of meta-list.
func (metaList *MetaList) Length() int {
	return len(metaList.list)
}

// MaybeBlockMetaContaining returns the block meta and the block index (block index starts from zero) that may contain the given key.
// It compares the key with the StartingKey of the block meta.
// It returns the instance of Meta where the given key is greater than or equal to the starting key.
func (metaList *MetaList) MaybeBlockMetaContaining(key kv.Key) (Meta, int) {
	low, high := 0, metaList.Length()-1
	possibleIndex := low
	for low <= high {
		mid := low + (high-low)/2
		meta := metaList.list[mid]
		switch key.CompareKeys(meta.StartingKey) { //TODO: replace compare with CompareWithTimestamp ..
		case -1:
			high = mid - 1
		case 0:
			return meta, mid
		case 1:
			possibleIndex = mid
			low = mid + 1
		}
	}
	return metaList.list[possibleIndex], possibleIndex
}

// DecodeToBlockMetaList decodes the MetaList from the byte slice.
// Please look at MetaList.Encode() to understand the encoding of MetaList.
func DecodeToBlockMetaList(buffer []byte, enableCompression bool) (*MetaList, error) {
	var decodedBuffer = buffer
	var err error

	if enableCompression {
		decodedBuffer, err = s2.Decode(nil, buffer)
		if err != nil {
			return nil, err
		}
	}
	numberOfBlocks := binary.LittleEndian.Uint32(decodedBuffer[:])
	blockList := make([]Meta, 0, numberOfBlocks)

	decodedBuffer = decodedBuffer[Uint32Size:]
	for blockCount := 0; blockCount < int(numberOfBlocks); blockCount++ {
		offset := binary.LittleEndian.Uint32(decodedBuffer[:])

		startingKeySize := binary.LittleEndian.Uint16(decodedBuffer[Uint32Size:])
		startingKeyBegin := 0 + Uint32Size + ReservedKeySize
		startingKey := decodedBuffer[startingKeyBegin : startingKeyBegin+int(startingKeySize)]

		endKeyBegin := 0 + startingKeyBegin + int(startingKeySize)
		endingKeySize := binary.LittleEndian.Uint16(decodedBuffer[endKeyBegin:])

		endKeyBegin = endKeyBegin + ReservedKeySize
		endingKey := decodedBuffer[endKeyBegin : endKeyBegin+int(endingKeySize)]

		blockList = append(blockList, Meta{
			BlockBeginOffset: offset,
			StartingKey:      kv.DecodeKeyFrom(startingKey),
			EndingKey:        kv.DecodeKeyFrom(endingKey),
		})
		index := endKeyBegin + int(endingKeySize)
		decodedBuffer = decodedBuffer[index:]
	}
	return &MetaList{
		list:              blockList,
		enableCompression: enableCompression,
	}, nil
}

// StartingKeyOfFirstBlock returns the starting key of the first block.
func (metaList *MetaList) StartingKeyOfFirstBlock() (kv.Key, bool) {
	if metaList.Length() > 0 {
		return metaList.list[0].StartingKey, true
	}
	return kv.Key{}, false
}

// EndingKeyOfLastBlock returns the ending key of the last block.
func (metaList *MetaList) EndingKeyOfLastBlock() (kv.Key, bool) {
	if metaList.Length() > 0 {
		return metaList.list[metaList.Length()-1].EndingKey, true
	}
	return kv.Key{}, false
}
