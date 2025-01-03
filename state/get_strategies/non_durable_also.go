package get_strategies

import "github.com/SarthakMakhija/zero-store/kv"

type NonDurableAlsoGet struct {
	nonDurableOnlyGetOperation NonDurableOnlyGet
	durableOnlyGetOperation    DurableOnlyGet
}

func NewNonDurableAlsoGet(nonDurableOnlyGetOperation NonDurableOnlyGet, durableOnlyGetOperation DurableOnlyGet) NonDurableAlsoGet {
	return NonDurableAlsoGet{
		nonDurableOnlyGetOperation: nonDurableOnlyGetOperation,
		durableOnlyGetOperation:    durableOnlyGetOperation,
	}
}

func (getOperation NonDurableAlsoGet) Get(key kv.Key) GetResponse {
	getResponse := getOperation.nonDurableOnlyGetOperation.Get(key)
	if getResponse.IsValueAvailable() {
		return getResponse
	}
	return getOperation.durableOnlyGetOperation.Get(key)
}
