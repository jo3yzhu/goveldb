package sstable

import (
	"github.com/jo3yzhu/goveldb/internal"
	"github.com/jo3yzhu/goveldb/sstable/block"
	"os"
)

const (
	MaxBlockSize = 4 * 1024
)

// NOTE: sstable know nothing about sorting, so is TableBuilder

type TableBuilder struct {
	file               *os.File
	offset             uint32             // current offset while writing
	numEntries         int32              // counter
	dataBlockBuilder   block.BlockBuilder // block builder for data block
	indexBlockBuilder  block.BlockBuilder // block builder for index block
	pendingIndexEntry  bool               // indicate a fresh block begin
	pendingIndexHandle IndexBlockHandle
	err                error
}


func NewTableBuilder(fileName string) *TableBuilder {
	var builder TableBuilder
	var err error
	builder.file, err = os.Create(fileName)
	if err != nil {
		return nil
	}
	builder.pendingIndexEntry = false
	return &builder
}

func (builder *TableBuilder) FileSize() uint32 {
	return builder.offset
}


func (builder *TableBuilder) Add(internalKey *internal.InternalKey) {
	if builder.err != nil {
		return
	}

	// the first time Add after flush and Finish leads to add internalKey to indexBlockBuilder
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}

	// TODO: filter block

	// updating pendingIndexHandle
	builder.pendingIndexHandle.InternalKey = internalKey

	// append data block
	builder.numEntries++
	builder.dataBlockBuilder.Add(internalKey)
	if builder.dataBlockBuilder.CurrentSizeEstimate() > MaxBlockSize {
		builder.flush()
	}
}

func (builder *TableBuilder) flush() {
	// no data to flush
	if builder.dataBlockBuilder.Empty() {
		return
	}

	// get last key of current data block and store it in the index block
	// but set the block's handle as its value
	orgKey := builder.pendingIndexHandle.InternalKey
	// dismiss value
	builder.pendingIndexHandle.InternalKey = internal.NewInternalKey(orgKey.Seq, orgKey.Type, orgKey.UserKey, nil)
	// write data block to file and set its handle to value
	builder.pendingIndexHandle.SetBlockHandle(builder.writeBlock(&builder.dataBlockBuilder))
	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) Finish() error {
	// write data block
	builder.flush()

	// write index block
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	var footer Footer
	footer.IndexHandle = builder.writeBlock(&builder.indexBlockBuilder)

	// write footer, footer needs to know where index block is
	_ = footer.EncodeTo(builder.file)
	_ = builder.file.Close()
	return nil
}

func (builder *TableBuilder) writeBlock(blockBuilder *block.BlockBuilder) BlockHandle {

	// Finish function will append nums of entries of block at end
	// It's not the BlockBuilder's Finish instead of TableBuilder's Finish
	content := blockBuilder.Finish()

	// TODO: compress and crc

	var blockHandle BlockHandle
	blockHandle.Offset = builder.offset
	blockHandle.Size = uint32(len(content))
	builder.offset += uint32(len(content))
	_, builder.err = builder.file.Write(content)
	_ = builder.file.Sync()
	blockBuilder.Reset()

	return blockHandle
}