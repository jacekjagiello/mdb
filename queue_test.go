package mdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	db, cleanup := TestLMDB(t)
	defer cleanup()

	queue := NewQueue(db)

	assert.NoError(t, queue.Enqueue("test"))
	assert.NoError(t, queue.Enqueue(123456))

	size, err := queue.Size()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), size)

	el, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, "test", el)

	el, err = queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, int64(123456), el)

	size, err = queue.Size()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), size)
}
