package get_strategies

import "github.com/SarthakMakhija/zero-store/kv"

type GetStrategyType int

const (
	NonDurableOnlyType GetStrategyType = 1
	DurableOnlyType    GetStrategyType = 2
	NonDurableAlsoType GetStrategyType = 3
)

type GetStrategy interface {
	Get(key kv.Key) GetResponse
}
