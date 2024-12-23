package key_cache

type keyId uint64

type timestampedKeyId struct {
	keyId
	timestamp uint64
}

func newTimestampedKeyId(keyId keyId, timestamp uint64) timestampedKeyId {
	return timestampedKeyId{
		keyId:     keyId,
		timestamp: timestamp,
	}
}

var compareKeysWithDescendingTimestamp = func(key, otherKey interface{}) int {
	this := key.(timestampedKeyId)
	other := otherKey.(timestampedKeyId)

	if this.keyId > other.keyId {
		return 1
	} else if this.keyId < other.keyId {
		return -1
	}
	if this.timestamp > other.timestamp {
		return -1
	} else if this.timestamp < other.timestamp {
		return 1
	}
	return 0
}
