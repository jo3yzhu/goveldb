package memtable

import (
	"goveldb/internal"
	"goveldb/skiplist"
)

type MemTable struct {
	table       *skiplist.SkipList
	memoryUsage uint64
}

func New() *MemTable {
	memtable := MemTable{
		table: skiplist.New(internal.InternalKeyComparator),
	}
	return &memtable
}

func (memTable *MemTable) Add(seq uint64, valueType internal.ValueType, key, value []byte) {
	internalKey := internal.NewInternalKey(seq, valueType, key, value)
	memTable.memoryUsage += uint64(16 + len(key) + len(value))
	memTable.table.Insert(internalKey)
}

func (memTable *MemTable) Get(key []byte) ([]byte, error) {
	// lookupKey is a key with max sequential number which means it's the smallest one in nodes with the same UserKey
	lookupKey := internal.LookupKey(key)
	iter := memTable.table.NewIterator()

	// Seek by lookupKey is to find the newest key with smallest sequential number
	iter.Seek(lookupKey)
	if iter.Valid() {
		internalKey := iter.Key().(*internal.InternalKey)
		if internal.UserKeyComparator(key, internalKey.UserKey) == 0 {
			if internalKey.Type == internal.TypeValue {
				return internalKey.UserValue, nil
			} else {
				return nil, internal.ErrDeletion // key doesn't exist
			}
		}
	}

	return nil, internal.ErrNotFound
}

func (memTable *MemTable) ApproximateMemoryUsage() uint64 {
	return memTable.memoryUsage
}

func (memTable *MemTable) NewIterator() *Iterator {
	return &Iterator{
		listIterator: memTable.table.NewIterator(),
	}
}
