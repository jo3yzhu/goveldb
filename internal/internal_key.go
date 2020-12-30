// keys:
// 		UserKey: the key provided by user in Put function
//		InternalKey: UserKey + SequentialNumber[0:56) for version + SequentialNumber[56:64) for type
//		LookupKey: the key provided by user in Get function
//
// InternalKey:
//		the actual content in memtable which include UserKey and UserValue, Type and increasing Seq per key

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

	// the binary.Write function write the data in the variable of interface{} into the writer in liitle endian
	write := func(w io.Writer, data interface{}) {
		if err != nil {
			return
		}
		err = binary.Write(w, binary.LittleEndian, data)
	}

	// the internal implementation is based on type switch
	write(w, key.Seq)
	write(w, key.Type)

	// the length of UserKey must be serialized in internal key
	write(w, int32(len(key.UserKey)))
	write(w, key.UserKey)

	// the length of UserValue must be serialized in internal key
	write(w, int32(len(key.UserValue)))
	write(w, key.UserValue)

	return err
}

// cannot use do while(false) trick in golang, awful
// description: decode an internal key from a readable object
// 				the read sequence is seq -> type -> length of key -> key without '/0' -> length of data -> data without '/0'
// return: the error

func (key *InternalKey) DecodeFrom(r io.Reader) error {
	var err error

	// the binary.Read function read the data from reader in little endian and put it into a interface{} variable
	read := func(r io.Reader, data interface{}) {
		if err != nil {
			return
		}

		err = binary.Read(r, binary.LittleEndian, data)
	}

	read(r, &key.Seq)
	read(r, &key.Type)

	var tmpLen int32
	read(r, &tmpLen)

	key.UserKey = make([]byte, tmpLen)
	read(r, &key.UserKey)

	read(r, &tmpLen)
	key.UserValue = make([]byte, tmpLen)
	read(r, &key.UserValue)

	return err
}

// @description: k-v pairs of leveldb is stored in skip list by InternalKey, when user need to index certain key, a temporary key need to be constructed,
// 				 but the key may not exist in skip list and it only provide std::lower_bound-like indexing interface, so the lookup key should maintain max seq and the same key
// @return: A InternalKey to lookup in skip list

func LookupKey(key []byte) *InternalKey {
	return NewInternalKey(math.MaxUint64, TypeValue, key, nil)
}

// @description: this function is to provide the rule of compare between two UserKey,
// @return: +1 if a > b, -1 if a < b, 0 if a == b
// @note: a, b would be compared lexicographically

func UserKeyComparator(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

// @description: this function is to provide the rule of compare between two InternalKey,
// @return: +1 if a > b, -1 if a < b
// 			if a == b, the node with greater seq is lesser

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
