package get_strategies

import (
	"errors"
	"github.com/SarthakMakhija/zero-store/kv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetResponseWithAValue(t *testing.T) {
	response := positiveResponse(kv.NewStringValue("raft"))
	assert.True(t, response.IsValueAvailable())
	assert.Equal(t, kv.NewStringValue("raft"), response.Value())
}

func TestGetResponseWithoutAValue(t *testing.T) {
	response := negativeResponse()
	assert.False(t, response.IsValueAvailable())
	assert.Equal(t, kv.EmptyValue, response.Value())
}

func TestGetResponseWithAnError(t *testing.T) {
	response := errorResponse(errors.New("test error"))
	assert.False(t, response.IsValueAvailable())
	assert.True(t, response.IsError())
}
