package mdb

import (
	"bytes"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
)

type RangeOptions struct {
	Limit   int
	Reverse bool
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

func (tx *Tx) GetRange(key []byte, opt RangeOptions) (kvs []KeyValue, err error) {
	scanner := lmdbscan.New(tx.Tx, tx.DB)
	defer scanner.Close()

	if !scanner.Set(key, nil, lmdb.SetRange) {
		return []KeyValue{}, nil
	}

	var scans int
	for scanner.Scan() {
		if !bytes.HasPrefix(scanner.Key(), key) {
			break
		}

		kvs = append(kvs, KeyValue{
			Key:   scanner.Key(),
			Value: scanner.Val(),
		})

		scans++
		if scans == opt.Limit {
			break
		}
	}

	// unfortunately reverse option requires reading all of the keys in given range
	// manually reversing the kvs slice, and shirking result to given limit
	// todo: find out if it's possible to achieve reverse using scanner API
	if opt.Reverse {
		for i, j := 0, len(kvs)-1; i < j; i, j = i+1, j-1 {
			kvs[i], kvs[j] = kvs[j], kvs[i]
		}
	}
	if opt.Reverse && opt.Limit != 0 {
		kvs = kvs[:opt.Limit]
	}

	return kvs, scanner.Err()
}

func (tx *Tx) ScanRange(key []byte, row func(k, v []byte) error) error {
	scanner := lmdbscan.New(tx.Tx, tx.DB)
	defer scanner.Close()

	if !scanner.Set(key, nil, lmdb.SetRange) {
		return nil
	}

	for scanner.Scan() {
		if !bytes.HasPrefix(scanner.Key(), key) {
			break
		}

		if err := row(scanner.Key(), scanner.Val()); err != nil {
			return err
		}
	}

	return scanner.Err()
}

func (tx *Tx) DelRange(key []byte) error {
	scanner := lmdbscan.New(tx.Tx, tx.DB)
	defer scanner.Close()

	if !scanner.Set(key, nil, lmdb.SetRange) {
		return nil
	}

	for scanner.Scan() {
		if !bytes.HasPrefix(scanner.Key(), key) {
			break
		}

		if err := tx.Tx.Del(tx.DB, scanner.Key(), nil); err != nil {
			return err
		}
	}

	return scanner.Err()
}
