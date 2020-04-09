package mdb

import (
	"github.com/pkg/errors"

	proto "github.com/golang/protobuf/proto"
)

func (tx *Tx) PutProto(key []byte, val proto.Message) error {
	var err error
	var data []byte

	if data, err = proto.Marshal(val); err != nil {
		return errors.Wrap(err, "Marshal")
	}
	return tx.Put(key, data)
}

func (tx *Tx) ReadProto(key []byte, pb proto.Message) error {
	var data []byte
	var err error

	if data, err = tx.Get(key); key != nil {
		return errors.Wrap(err, "tx.Get")
	}

	if data == nil {
		return nil
	}

	if err = proto.Unmarshal(data, pb); err != nil {
		return errors.Wrap(err, "Unmarshal")
	}
	return nil
}
