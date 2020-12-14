package block

import (
	"bytes"
	"encoding/binary"
	"goveldb/internal"
)

type BlockBuilder struct {
	buf bytes.Buffer
	counter uint32
}

func (blockbuilder *BlockBuilder) Reset() {
	blockbuilder.counter = 0
	blockbuilder.buf.Reset() // resets the buffer to be empty
}

func (blockbuilder *BlockBuilder) Add(item *internal.InternalKey) {
	blockbuilder.counter++
	item.EncodeTo(&blockbuilder.buf) // the Writer implementation in bytes.Buffer use pointer receiver
}

func (blockbuilder *BlockBuilder) Finish() []byte {
	binary.Write(&blockbuilder.buf, binary.LittleEndian, blockbuilder.counter) // the Writer implementation in bytes.Buffer use pointer receiver
	return blockbuilder.buf.Bytes()
}

func (blockbuilder *BlockBuilder) CurrentSizeEstimate() int {
	return blockbuilder.buf.Len()
}

func (blockbuilder *BlockBuilder) Empty() bool {
	return blockbuilder.buf.Len() == 0
}
