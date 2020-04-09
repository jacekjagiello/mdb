package mdb

import (
	"testing"

	"github.com/abdullin/lex-go/tuple"
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
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 4 {
		t.Errorf("expected 4 elements, got %d", len(kvs))
	}

	kvs, err = tx.GetRange(CreateKey("sub"), RangeOptions{Limit: 4, Reverse: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 4 {
		t.Errorf("expected 4 elements, got %d", len(kvs))
	}
}
