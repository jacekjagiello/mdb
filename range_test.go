package mdb

import (
	"testing"

	"github.com/abdullin/lex-go/tuple"

	"github.com/stretchr/testify/assert"
)

func TestTx_GetRange(t *testing.T) {
	db, tx := NewDbWithKeys(t, []tuple.Tuple{
		{"sub", 0},
		{"sub", 2},
		{"sub", 4},
		{"sub", 6},
		{"sub", 8},
	})
	defer db.Close()

	kvs, err := tx.GetRange(CreateKey("sub"), RangeOptions{Limit: 4})
	assert.NoError(t, err)
	assert.Len(t, kvs, 4)
	assert.Equal(t, tuple.Tuple{"sub", int64(0)}, getKey(t, kvs[0].Key))
	assert.Equal(t, tuple.Tuple{"sub", int64(2)}, getKey(t, kvs[1].Key))
	assert.Equal(t, tuple.Tuple{"sub", int64(4)}, getKey(t, kvs[2].Key))
	assert.Equal(t, tuple.Tuple{"sub", int64(6)}, getKey(t, kvs[3].Key))

	kvs, err = tx.GetRange(CreateKey("sub"), RangeOptions{Limit: 4, Reverse: true})
	assert.NoError(t, err)
	assert.Len(t, kvs, 4)
	assert.Equal(t, tuple.Tuple{"sub", int64(8)}, getKey(t, kvs[0].Key))
	assert.Equal(t, tuple.Tuple{"sub", int64(6)}, getKey(t, kvs[1].Key))
	assert.Equal(t, tuple.Tuple{"sub", int64(4)}, getKey(t, kvs[2].Key))
	assert.Equal(t, tuple.Tuple{"sub", int64(2)}, getKey(t, kvs[3].Key))
}

func getKey(t *testing.T, v []byte) tuple.Tuple {
	key, err := tuple.Unpack(v)
	assert.NoError(t, err)
	return key
}
