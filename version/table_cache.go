package version

import (
	"github.com/hashicorp/golang-lru"
	"github.com/jo3yzhu/goveldb/internal"
	"github.com/jo3yzhu/goveldb/sstable"
	"sync"
)

// TableCache is used to cache several sstables in memory of one database file
// TableCache is also

type TableCache struct {
	mu     sync.Mutex // golang-lru is thread-safe, but still need to protect local file in findTable
	dbName string     // a database contains many sstables
	cache  *lru.Cache // key is file number of sstable, value is *sstable
}

func NewTableCache(dbName string) *TableCache {
	c, _ := lru.New(internal.MaxOpenFiles - internal.NumNonTableCacheFiles) // lru cache size
	return &TableCache{
		dbName: dbName,
		cache:  c,
	}
}

// @description: get a sstable by its file number, maybe in cache or disk and then loaded in cache
// @param: file number, in other words, file name
// @return: pointer of sstable and error if any
// @notice: all sstable file name is generated by file number

func (tableCache *TableCache) findTable(fileNum uint64) (*sstable.SsTable, error) {
	tableCache.mu.Lock()
	defer tableCache.mu.Unlock()

	// if already exists, return it
	if table, ok := tableCache.cache.Get(fileNum); ok {
		return table.(*sstable.SsTable), nil
	} else {
		// if sstable with fileNum doesn't exist in lru, add it in cache and return
		ssTable, err := sstable.Open(internal.TableFileName(tableCache.dbName, fileNum))
		tableCache.cache.Add(fileNum, ssTable)
		return ssTable, err
	}
}

// @description: get a iterator of sstable in table cache
// @param: file number, in other words, file name
// @return: the iterator of the sstable, if any error return nil

func (tableCache *TableCache) NewIterator(fileNum uint64) *sstable.Iterator {
	if table, _ := tableCache.findTable(fileNum); table != nil {
		return table.NewIterator()
	}

	return nil
}

// @description: get value of key in sstable with file number
// @param: file number, in other words, file name and key
// @return: value and error

func (tableCache *TableCache) Get(fileNum uint64, key []byte) ([]byte, error) {
	table, err := tableCache.findTable(fileNum)
	if table != nil {
		return table.Get(key)
	}

	return nil, err
}

// @description: erase a sstable with file in cache
// @param: file number, in other words, file name

func (tableCache *TableCache) Evict(fileNum uint64) {
	tableCache.cache.Remove(fileNum)
}
