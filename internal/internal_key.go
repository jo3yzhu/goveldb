// keys:
// 		UserKey: the key provided by user in Put function
//		InternalKey: UserKey + SequentialNumber[0:56) for version + SequentialNumber[56:64) for type
//		LookupKey: the key provided by user in Get function

package internal

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

type ValueType int8

const (
	TypeDeletion ValueType = 0 // lazy deletion
	TypeValue    ValueType = 1
)

type InternalKey struct {
	Seq       uint64
	Type      ValueType
	UserKey   []byte
	UserValue []byte
}

func NewInternalKey(seq uint64, valueType ValueType, key, value []byte) *InternalKey {
	internalKey := InternalKey{
		Seq:  seq,
		Type: valueType,
	}

	// deep copy for slice which use reference semantic by default in case of unexpected modification
	internalKey.UserKey = make([]byte, len(key))
	copy(internalKey.UserKey, key)

	internalKey.UserValue = make([]byte, len(value))
	copy(internalKey.UserValue, value)

	return &internalKey
}

// cannot use do while(false) trick in golang, awful
// @description: encode a internal key in little endian binary to a writable object which has a Write(p []byte) (n int, err error) function
// 				 the write sequence is seq -> type -> length of key -> key without '/0' -> length of data -> data without '/0'
// @return the error

func (key *InternalKey) EncodeTo(w io.Writer) error {
	var err error = nil

	if err = binary.Write(w, binary.LittleEndian, key.Seq); err != nil {
		return err
	}
	if err = binary.Write(w, binary.LittleEndian, key.Type); err != nil {
		return err
	}
	if err = binary.Write(w, binary.LittleEndian, int32(len(key.UserKey))); err != nil {
		return err
	}
	if err = binary.Write(w, binary.LittleEndian, key.UserKey); err != nil {
		return err
	}
	if err = binary.Write(w, binary.LittleEndian, int32(len(key.UserValue))); err != nil {
		return err
	}
	if err = binary.Write(w, binary.LittleEndian, key.UserValue); err != nil {
		return err
	}

	return err
}

// cannot use do while(false) trick in golang, awful
// description: decode an internal key from a readable object
// 				the read sequence is seq -> type -> length of key -> key without '/0' -> length of data -> data without '/0'
// return: the error

func (key *InternalKey) DecodeFrom(r io.Reader) error {
	var err error
	if err = binary.Read(r, binary.LittleEndian, &key.Seq); err != nil {
		return err
	}
	if err = binary.Read(r, binary.LittleEndian, &key.Type); err != nil {
		return err
	}
	var tmpLen int32
	if err = binary.Read(r, binary.LittleEndian, &tmpLen); err != nil {
		return err
	}
	key.UserKey = make([]byte, tmpLen)
	if err = binary.Read(r, binary.LittleEndian, &key.UserKey); err != nil {
		return err
	}
	if err = binary.Read(r, binary.LittleEndian, &tmpLen); err != nil {
		return err
	}
	key.UserValue = make([]byte, tmpLen)
	if err = binary.Read(r, binary.LittleEndian, &key.UserValue); err != nil {
		return err
	}

	return err
}

func LookupKey(key []byte) *InternalKey {
	return NewInternalKey(math.MaxUint64, TypeValue, key, nil)
}

// @description: this function is to provide the rule of compare between two UserKey,
// @return: +1 if a > b, -1 if a < b, 0 if a == b

func UserKeyComparator(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

// @description: this function is to provide the rule of compare between two InternalKey,
// @return: +1 if a > b, -1 if a < b
// 			if a == b, the node with greater is lesser

func InternalKeyComparator(a, b interface{}) int {
	aKey := a.(*InternalKey)
	bKey := b.(*InternalKey)
	r := UserKeyComparator(aKey.UserKey, bKey.UserKey)
	if r == 0 {
		if aKey.Seq > bKey.Seq {
			r = -1
		} else if aKey.Seq < bKey.Seq {
			r = 1
		}
	}

	return r
}
