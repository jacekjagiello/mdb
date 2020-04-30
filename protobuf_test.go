package mdb

import (
	"testing"
)

type exampleStruct struct {
	String string
	Number int64
	Bool bool
	bytes []byte
}

func TestTx_PutGob_ReadGob(t *testing.T) {
	db, tx := NewDbWithRange(t, 10)
	defer db.Close()

	obj := exampleStruct{
		String: "test",
		Number: 123,
		Bool:   true,
		bytes:  []byte("test"),
	}

	if err := tx.PutGob([]byte("obj/1"), obj); err != nil {
		t.Fatal(err)
	}

	readObj := exampleStruct{}
	if err := tx.ReadGob([]byte("obj/1"), &readObj); err != nil {
		t.Fatal(err)
	}

	if readObj.String != obj.String {
		t.Fatal("string don't match")
	}
}
