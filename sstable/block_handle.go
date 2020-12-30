package sstable

import (
	"encoding/binary"
	"goveldb/internal"
	"io"
)

type BlockHandle struct {
	Offset uint32
	Size   uint32
}

const (
	kTableMagicNumber uint64 = 0xdb4775248b80fb57
)

// @description: encode blockHandle into a byte slice
// @return: the byte slice

func (blockHandle *BlockHandle) EncodeToBytes() []byte {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint32(p, blockHandle.Offset)
	binary.LittleEndian.PutUint32(p[4:], blockHandle.Size)
	return p
}

// @description: decode blockHandle itself from a byte slice
// @param: the byte slice

func (blockHandle *BlockHandle) DecodeFromBytes(p []byte) {
	if len(p) == 8 {
		blockHandle.Offset = binary.LittleEndian.Uint32(p)
		blockHandle.Size = binary.LittleEndian.Uint32(p[4:])
	}
}

// IndexBlockHandle is a simple pack of internalKey
// IndexBlockHandle is used to store BlockHandle in byte slice in its UserValue

type IndexBlockHandle struct {
	*internal.InternalKey
}

func (index *IndexBlockHandle) SetBlockHandle(blockHandle BlockHandle) {
	index.UserValue = blockHandle.EncodeToBytes()
}

func (index *IndexBlockHandle) GetBlockHandle() (blockHandle BlockHandle) {
	blockHandle.DecodeFromBytes(index.UserValue)
	return
}

// In sstable, the footer contains the index to MetaBlockIndex and DataBlockIndex

type Footer struct {
	MetaIndexHandle BlockHandle
	IndexHandle     BlockHandle
}

func (footer *Footer) Size() int {
	return binary.Size(footer) + 8 // 8 bytes for magic, golang read size of user defined by reflect
}

// @description: encode the footer itself into a writer
// @param: the writable object

func (footer *Footer) EncodeTo(w io.Writer) error {
	// 1. write meta and data handle to file
	err := binary.Write(w, binary.LittleEndian, footer) // user defined type can be written to binary buffer by reflect of golang
	if err != nil {
		return err
	}

	// 2. write magic number to file
	err = binary.Write(w, binary.LittleEndian, kTableMagicNumber)
	return err
}

// @description: decode the footer itself from a reader
// @param: the readable object

func (footer *Footer) DecodeFrom(r io.Reader) error {
	err := binary.Read(r, binary.LittleEndian, footer) // user defined type can be read from binary buffer by reflect of golang
	if err != nil {
		return err
	}

	var magic uint64
	err = binary.Read(r, binary.LittleEndian, &magic)

	if err != nil {
		return err
	}

	if magic != kTableMagicNumber {
		return internal.ErrTableFileMagic
	}

	return nil
}
