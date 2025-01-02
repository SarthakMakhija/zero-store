package get_strategies

import "github.com/SarthakMakhija/zero-store/kv"

type GetResponse struct {
	value kv.Value
	found bool
	err   error
}

func positiveResponse(value kv.Value) GetResponse {
	return GetResponse{
		value: value,
		found: true,
	}
}

func negativeResponse() GetResponse {
	return GetResponse{
		value: kv.EmptyValue,
		found: false,
	}
}

func errorResponse(err error) GetResponse {
	return GetResponse{
		value: kv.EmptyValue,
		found: false,
		err:   err,
	}
}

func (response GetResponse) IsValueAvailable() bool {
	return !response.value.IsEmpty()
}

func (response GetResponse) IsError() bool {
	return response.err != nil
}

func (response GetResponse) Value() kv.Value {
	return response.value
}
