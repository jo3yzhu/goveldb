package sstable

import (
	"github.com/jo3yzhu/goveldb/internal"
	"github.com/jo3yzhu/goveldb/sstable/block"
	"io"
	"os"
)

type SsTable struct {
	index  *block.Block // sstable has a unique index block indicate where data block is
	footer Footer
	file   *os.File
}

// @description: read a block from disk by block handle
// @param: the block handle with information where the block is and how long it is
// @return: the block

func (table *SsTable) readBlock(handle BlockHandle) *block.Block {
	p := make([]byte, handle.Size)
	n, err := table.file.ReadAt(p, int64(handle.Offset))
	if err != nil || uint32(n) != handle.Size {
		return nil
	}

	return block.New(p)
}

func Open(fileName string) (*SsTable, error) {
	var table SsTable
	var err error

	table.file, err = os.Open(fileName)
	if err != nil {
		return nil, err
	}

	// 1. in case file is too short
	stat, _ := table.file.Stat()
	footerSize := int64(table.footer.Size())
	if stat.Size() < footerSize {
		return nil, internal.ErrTableTooShort
	}

	// 2. read footer block
	_, err = table.file.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	err = table.footer.DecodeFrom(table.file)
	if err != nil {
		return nil, err
	}

	// 3. read index block
	table.index = table.readBlock(table.footer.IndexHandle)

	// TODO: read meta block

	return &table, nil
}

func (table *SsTable) NewIterator() *Iterator {
	return &Iterator{
		table:     table,
		indexIter: table.index.NewIterator(),
	}
}

func (table *SsTable) Get(target [] byte) ([]byte, error) {
	iter := table.NewIterator()
	iter.Seek(target)

	if iter.Valid() {
		internalKey := iter.InternalKey()
		if internal.UserKeyComparator(internalKey.UserKey, target) == 0 {
			if internalKey.Type == internal.TypeValue {
				return internalKey.UserValue, nil
			} else {
				return nil, internal.ErrDeletion
			}
		}
	}

	return nil, internal.ErrNotFound
}
