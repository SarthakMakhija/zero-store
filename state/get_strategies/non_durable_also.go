package get_strategies

import "github.com/SarthakMakhija/zero-store/kv"

type nonDurableAlsoGet struct {
	nonDurableOnlyGetOperation nonDurableOnlyGet
	durableOnlyGetOperation    durableOnlyGet
}

func newNonDurableAlsoGet(nonDurableOnlyGetOperation nonDurableOnlyGet, durableOnlyGetOperation durableOnlyGet) nonDurableAlsoGet {
	return nonDurableAlsoGet{
		nonDurableOnlyGetOperation: nonDurableOnlyGetOperation,
		durableOnlyGetOperation:    durableOnlyGetOperation,
	}
}

func (getOperation nonDurableAlsoGet) get(key kv.Key) GetResponse {
	getResponse := getOperation.nonDurableOnlyGetOperation.get(key)
	if getResponse.IsValueAvailable() {
		return getResponse
	}
	return getOperation.durableOnlyGetOperation.get(key)
}
