package state

type StorageOptions struct {
	sortedSegmentSizeInBytes int64
	maximumInactiveSegments  uint
}

type StorageOptionsBuilder struct {
	sortedSegmentSizeInBytes int64
	maximumInactiveSegments  uint
}

func NewStorageOptionsBuilder() *StorageOptionsBuilder {
	return &StorageOptionsBuilder{
		sortedSegmentSizeInBytes: 1 << 20,
		maximumInactiveSegments:  8,
	}
}

func (builder *StorageOptionsBuilder) WithSortedSegmentSizeInBytes(size int64) *StorageOptionsBuilder {
	if size < 0 {
		panic("sorted segment size must be greater than 0")
	}
	//TODO: what if size is too less like 10 bytes?
	builder.sortedSegmentSizeInBytes = size
	return builder
}

func (builder *StorageOptionsBuilder) WithMaximumInactiveSegments(inactiveSegments uint) *StorageOptionsBuilder {
	builder.maximumInactiveSegments = inactiveSegments
	return builder
}

func (builder *StorageOptionsBuilder) Build() StorageOptions {
	return StorageOptions{
		sortedSegmentSizeInBytes: builder.sortedSegmentSizeInBytes,
		maximumInactiveSegments:  builder.maximumInactiveSegments,
	}
}
