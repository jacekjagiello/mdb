package mdb

import (
	"bytes"
	"encoding/binary"

	"github.com/abdullin/lex-go/tuple"
)

type Queue struct {
	db *DB

	sizeSubspace     []byte
	elementsSubspace []byte
}

func (q *Queue) Enqueue(elements ...interface{}) error {
	return q.db.Update(func(tx *Tx) error {
		for _, element := range elements {
			nextIndex, err := q.size(tx)
			if err != nil {
				return err
			}

			sizeBytes, err := uint64ToBytes(nextIndex)
			if err != nil {
				return err
			}

			if err := tx.Put(concat(q.elementsSubspace, sizeBytes), tuple.Tuple{element}.Pack()); err != nil {
				return err
			}

			if err := tx.Increment(q.sizeSubspace); err != nil {
				return err
			}
		}

		return nil
	})
}

func (q *Queue) Dequeue() (element interface{}, err error) {
	err = q.db.Update(func(tx *Tx) error {
		key, value, err := tx.FirstFromRange(q.elementsSubspace)
		if err != nil {
			return err
		}

		element = value

		if err = tx.Del(key); err != nil {
			return err
		}

		return tx.Decrement(q.sizeSubspace)
	})

	return element, err
}

func (q *Queue) Size() (size uint64, err error) {
	err = q.db.Read(func(tx *Tx) error {
		size, err = q.size(tx)
		return err
	})

	return size, err
}

func (q *Queue) size(tx *Tx) (uint64, error) {
	sizeBytes, err := tx.Get(q.sizeSubspace)
	if err != nil {
		return 0, err
	}

	if bytes.Equal(sizeBytes, []byte{}) {
		return 0, nil
	}


	return bytesToUint64(sizeBytes), nil
}

func NewQueue(db *DB) *Queue {
	return &Queue{db, []byte("size"), []byte("elements")}
}

func concat(a []byte, b []byte) []byte {
	r := make([]byte, len(a)+len(b))
	copy(r, a)
	copy(r[len(a):], b)
	return r
}

func uint64ToBytes(number uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, number)

	return buf.Bytes(), err
}

func bytesToUint64(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}
