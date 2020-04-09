package mdb

import (
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
