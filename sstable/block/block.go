package block

import (
	"bytes"
	"encoding/binary"
	"goveldb/internal"
)

// a block contains internal keys and is stored in disk
type Block struct {
	items []internal.InternalKey
}

func New(p []byte) *Block {
	var block Block

	// the last 4 bytes in block represent the key num of the block
	// instead of restart point in leveldb, I keep it simple here
	keyNum := binary.LittleEndian.Uint32(p[len(p)-4:])

	// bytes.Buffer has implemented the Reader interface
	data := bytes.NewBuffer(p)

	for i := uint32(0); i < keyNum; i++ {
		var internalKey internal.InternalKey
		err := internalKey.DecodeFrom(data)
		if err != nil {
			return nil
		}

		block.items = append(block.items, internalKey)
	}

	return &block
}

func (block *Block) NewIterator() *Iterator {
	return &Iterator{
		block: block,
	}
}
