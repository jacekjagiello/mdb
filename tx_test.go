package mdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNext(t *testing.T) {
	db, tx := NewDbWithRange(t, 10)
	defer db.Close()

	k, v, err := tx.GetNext(CreateKey(5))
	if err != nil {
		t.Fatal("Failed to find", err)
	}

	dk, dv := decodeFirstAsInt(k), decodeFirstAsInt(v)
	if dk != 6 || dv != 6 {
		t.Fatal("Expected key/value 6/6", "got", dk, dv)
	}
}

func TestGetPrev(t *testing.T) {
	db, tx := NewDbWithRange(t, 10)
	defer db.Close()

	k, v, err := tx.GetPrev(CreateKey(5))
	if err != nil {
		t.Fatal("Failed to find", err)
	}

	dk, dv := decodeFirstAsInt(k), decodeFirstAsInt(v)
	if dk != 4 || dv != 4 {
		t.Fatal("Expected key/value 4/4", "got", dk, dv)
	}
}

func TestTx_Increment_Decrement(t *testing.T) {
	db, tx := NewDbWithRange(t, 10)
	defer db.Close()

	key := []byte("key")

	assert.NoError(t, tx.Increment(key))
	assert.NoError(t, tx.Increment(key))
	assert.NoError(t, tx.Increment(key))
	assert.NoError(t, tx.Decrement(key))

	data, err := tx.Get(key)
	assert.NoError(t, err)

	assert.Equal(t, uint64(2), bytesToUint64(data))
}
