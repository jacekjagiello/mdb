package mdb

import (
	"testing"
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
