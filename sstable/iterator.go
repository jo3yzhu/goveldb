package sstable

import (
	"github.com/jo3yzhu/goveldb/internal"
	"github.com/jo3yzhu/goveldb/sstable/block"
)

type Iterator struct {
	table           *SsTable
	dataBlockHandle BlockHandle // the data block handle of current key
	dataIter        *block.Iterator
	indexIter       *block.Iterator
}

func (iter *Iterator) Valid() bool {
	return iter.dataIter != nil && iter.dataIter.Valid()
}

func (iter *Iterator) InternalKey() *internal.InternalKey {
	return iter.dataIter.InternalKey()
}

func (iter *Iterator) Key() []byte {
	return iter.InternalKey().UserKey
}

func (iter *Iterator) Value() []byte {
	return iter.InternalKey().UserValue
}

func (iter *Iterator) initDataBlock() {
	if !iter.indexIter.Valid() {
		iter.dataIter = nil
	} else {
		index := IndexBlockHandle{
			InternalKey: iter.indexIter.InternalKey(),
		}

		dataBlockHandle := index.GetBlockHandle()

		if iter.dataIter != nil && iter.dataBlockHandle == dataBlockHandle {
			// nothing to do
		} else {
			iter.dataIter = iter.table.readBlock(dataBlockHandle).NewIterator()
			iter.dataBlockHandle = dataBlockHandle
		}
	}
}

func (iter *Iterator) skipEmptyDataBlocksForward() {
	for iter.dataIter == nil || !iter.dataIter.Valid() {
		if !iter.indexIter.Valid() {
			iter.dataIter = nil
			return
		}
		iter.indexIter.Next()
		iter.initDataBlock()
		if iter.dataIter != nil {
			iter.dataIter.SeekToFirst()
		}
	}
}

func (iter *Iterator) skipEmptyDataBlocksBackward() {
	for iter.dataIter == nil || !iter.dataIter.Valid() {
		if !iter.indexIter.Valid() {
			iter.dataIter = nil
			return
		}
		iter.indexIter.Prev()
		iter.initDataBlock()
		if iter.dataIter != nil {
			iter.dataIter.SeekToLast()
		}
	}
}

func (iter *Iterator) Seek(target []byte) {
	// indexIter's key is the largest key of data block it managed (details in table_build)
	iter.indexIter.Seek(target)

	// init data block by indexIter
	iter.initDataBlock()

	// now we can assert that dataIter's block may contain the target
	if iter.dataIter != nil {
		iter.dataIter.Seek(target)
	}

	iter.skipEmptyDataBlocksBackward()
}

func (iter *Iterator) SeekToFirst() {
	iter.indexIter.SeekToFirst()
	iter.initDataBlock()
	if iter.dataIter != nil {
		iter.dataIter.SeekToFirst()
	}
	iter.skipEmptyDataBlocksForward()
}

func (iter *Iterator) SeekToLast() {
	iter.indexIter.SeekToLast()
	iter.initDataBlock()
	if iter.dataIter != nil {
		iter.dataIter.SeekToLast()
	}
	iter.skipEmptyDataBlocksBackward()
}

func (iter *Iterator) Next() {
	iter.dataIter.Next()
	iter.skipEmptyDataBlocksForward()
}

func (iter *Iterator) Prev() {
	iter.dataIter.Prev()
	iter.skipEmptyDataBlocksBackward()
}
