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

	// the MSB 4 bytes represent the key num of the block
	keyNum := binary.LittleEndian.Uint32(p[len(p)-4:])

	// bytes.Buffer has implemented the Reader interface
	data := bytes.NewBuffer(p)

	for i := uint32(0); i < keyNum; i++ {
		var internalKey internal.InternalKey
		err := internalKey.DecodeFrom(data)
		if err != nil {
			block.items = append(block.items, internalKey)
		}
	}

	return &block;
}
