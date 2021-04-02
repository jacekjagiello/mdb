package mdb

import (
	"os"
	"testing"

	"github.com/abdullin/lex-go/tuple"
)

var testFileName = "./test-lmdb"

func NewDbWithRange(t *testing.T, max int) (*DB, *Tx) {
	db, err := New(testFileName, "testdb", NewConfig())
	if err != nil {
		t.Fatal(err)
	}

	var tx *Tx
	if tx, err = db.CreateWrite(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < max; i += 2 {
		key := CreateKey(i)
		err := tx.Put(key, key)
		if err != nil {
			t.Fatal(err)
		}
	}

	return db, tx
}

func NewDbWithKeys(t *testing.T, keys []tuple.Tuple) (*DB, *Tx) {
	db, err := New(testFileName, "testdb", NewConfig())
	if err != nil {
		t.Fatal(err)
	}

	var tx *Tx
	if tx, err = db.CreateWrite(); err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		tx.Put(key.Pack(), key.Pack())
	}

	return db, tx
}

func NewDB(t *testing.T) *DB {
	db, err := New(testFileName, "testdb", NewConfig())
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func decodeFirstAsInt(b []byte) int64 {
	tpl, err := tuple.Unpack(b)
	if err != nil {
		panic(err)
	}

	return tpl[0].(int64)
}


// TestLMDB creates new instance of *mdb.DB, for test purposes
func TestLMDB(t testing.TB) (*DB, func()) {
	db, err := New(".tmp", "test", NewConfig())
	if err != nil {
		t.Fatal(err)
	}

	return db, func() {
		if err = os.RemoveAll(".tmp"); err != nil {
			t.Fatal(err)
		}
	}
}
