package mdb

import (
	"bytes"
	"fmt"

	"github.com/abdullin/lex-go/tuple"
	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
	"github.com/pkg/errors"
)

type Tx struct {
	DB  lmdb.DBI
	Env *lmdb.Env
	Tx  *lmdb.Txn
}

func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

func (tx *Tx) Put(key []byte, val []byte) error {
	if err := tx.Tx.Put(tx.DB, key, val, 0); err != nil {
		return errors.Wrap(err, "tx.Put")
	}
	return nil
}

func (tx *Tx) Del(key []byte) error {
	if err := tx.Tx.Del(tx.DB, key, nil); err != nil {
		return err

	}
	return nil
}

func (tx *Tx) PutReserve(key []byte, size int) ([]byte, error) {
	return tx.Tx.PutReserve(tx.DB, key, size, 0)
}

func (tx *Tx) Close() (err error) {
	return nil
}

func (tx *Tx) Get(key []byte) (data []byte, err error) {
	if data, err = tx.Tx.Get(tx.DB, key); err != nil {
		if lmdb.IsNotFound(err) {
			return nil, nil
		}

		return nil, errors.Wrap(err, "Tx.Get")
	}
	return data, nil
}

func (tx *Tx) GetNext(key []byte) (k, v []byte, err error) {
	scanner := lmdbscan.New(tx.Tx, tx.DB)
	defer scanner.Close()
	if !scanner.Set(key, nil, lmdb.SetRange) {
		err = lmdb.NotFound
		return
	}

	if !scanner.Scan() {
		err = lmdb.NotFound
		return
	}

	k = scanner.Key()
	v = scanner.Val()
	err = scanner.Err()
	return
}

func (tx *Tx) GetPrev(key []byte) (k, v []byte, err error) {
	scanner := lmdbscan.New(tx.Tx, tx.DB)
	defer scanner.Close()
	if !scanner.Set(key, nil, lmdb.SetRange) {
		err = lmdb.NotFound
		return
	}
	if !scanner.Set(nil, nil, lmdb.Prev) {
		err = lmdb.NotFound
		return
	}

	if !scanner.Scan() {
		err = lmdb.NotFound
		return
	}

	k = scanner.Key()
	v = scanner.Val()
	err = scanner.Err()
	return
}

func (tx *Tx) FirstFromRange(key []byte) (k []byte, v interface{}, err error) {
	kvs, err := tx.GetRange(key, RangeOptions{Limit: 1})
	if err != nil {
		return []byte{}, []byte{}, err
	}

	fmt.Printf("%+v", kvs[0].Value)

	valTuple, err := tuple.Unpack(kvs[0].Value)
	if err != nil {
		return []byte{}, []byte{}, err
	}

	return kvs[0].Key, valTuple[0], nil
}

func (tx *Tx) Increment(key []byte) error {
	currentValueBytes, err := tx.Get(key)
	if err != nil {
		return err
	}

	var currentValue uint64
	if !bytes.Equal(currentValueBytes, []byte{}) {
		currentValue = bytesToUint64(currentValueBytes)
	}

	newValue, err := uint64ToBytes(currentValue + 1)
	if err != nil {
		return err
	}

	return tx.Put(key, newValue)
}

func (tx *Tx) Decrement(key []byte) error {
	currentValueBytes, err := tx.Get(key)
	if err != nil {
		return err
	}

	if bytes.Equal(currentValueBytes, []byte{}) {
		val, err := uint64ToBytes(0)
		if err != nil {
			return err

		}
		return tx.Put(key, val)
	}

	newValue, err := uint64ToBytes(bytesToUint64(currentValueBytes) - 1)
	if err != nil {
		return err
	}

	return tx.Put(key, newValue)
}
