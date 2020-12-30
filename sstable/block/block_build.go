package block

import (
	"bytes"
	"encoding/binary"
	"goveldb/internal"
)

// a block contains several internal keys, which are in order by key
// A block builder in leveldb use restart pointer to compress
type BlockBuilder struct {
	buf bytes.Buffer // maintains a buffer can be added while Add
	counter uint32 // keep it simple, no restart point
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
	// write the counter to buf and return it
	binary.Write(&blockbuilder.buf, binary.LittleEndian, blockbuilder.counter) // the Writer implementation in bytes.Buffer use pointer receiver
	return blockbuilder.buf.Bytes()
}

func (blockbuilder *BlockBuilder) CurrentSizeEstimate() int {
	return blockbuilder.buf.Len()
}

func (blockbuilder *BlockBuilder) Empty() bool {
	return blockbuilder.buf.Len() == 0
}
