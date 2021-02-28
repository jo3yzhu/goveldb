package db

import (
	"fmt"
	"github.com/jo3yzhu/goveldb/internal"
	"github.com/jo3yzhu/goveldb/memtable"
	"github.com/jo3yzhu/goveldb/version"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"
)

type Db struct {
	name                  string
	mu                    sync.Mutex // no MVCC is implemented here, so we need a mutex to make Get, Put and Delete exclusive
	cond                  *sync.Cond // indicate that minor compaction is finished
	mem                   *memtable.MemTable
	imm                   *memtable.MemTable
	current               *version.Version
	bgCompactionScheduled bool // indicate that if there is a compaction processing
}

// @description: current file in leveldb knows which the newest manifest file
//               when database is restarted, ask current file for it

func (db *Db) SetCurrentFile(descriptorNumber uint64) {
	temp := internal.TempFileName(db.name, descriptorNumber)
	ioutil.WriteFile(temp, []byte(fmt.Sprintf("%d", descriptorNumber)), 0600)
	os.Rename(temp, internal.CurrentFileName(db.name))
}

func (db *Db) ReadCurrentFile() uint64 {
	b, err := ioutil.ReadFile(internal.CurrentFileName(db.name))
	if err != nil {
		return 0
	}
	descriptorNumber, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}

	return descriptorNumber
}

func (db *Db) backgroundCompaction() {
	imm := db.imm
	v := db.current.Copy()
	db.mu.Unlock()

	// minor compaction
	if imm != nil {
		v.WriteLevel0Table(imm)
	}

	// major compaction
	for v.DoCompactionWork() {
		v.Log()
	}

	descriptorNumber, _ := v.Save()
	db.SetCurrentFile(descriptorNumber)

	// TODO: maybe better
	db.mu.Lock()
	db.imm = nil
	db.current = v
}

func (db *Db) backgroundCall() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.backgroundCompaction()
	db.bgCompactionScheduled = false
	db.cond.Broadcast()
}

func (db *Db) maybeScheduleCompaction() {
	if db.bgCompactionScheduled {
		return
	}
	db.bgCompactionScheduled = true
	go db.backgroundCall()
}

func (db *Db) makeRoomForWrite() (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for true {
		// if there are too many files in level0, slow it down
		if db.current.NumLevelFiles(0) >= internal.L0SlowdownWriteTrigger {
			db.mu.Unlock()
			time.Sleep(time.Duration(1000) * time.Microsecond)
			db.mu.Lock()
			continue
		}

		// if there is room for data in memtable, just write it
		if db.mem.ApproximateMemoryUsage() <= internal.WriteBufferSize {
			return db.current.NextSeq(), nil
		}

		// memtable is full and immutable has not been compacted, wait until compaction is finished
		if db.imm != nil {
			db.cond.Wait()
		} else {
			db.imm = db.mem
			db.mem = memtable.New()
			db.maybeScheduleCompaction()
		}
	}

	return db.current.NextSeq(), nil
}

func Open(dbName string) *Db {
	var db Db
	db.name = dbName
	db.mem = memtable.New()
	db.imm = nil
	db.bgCompactionScheduled = false
	db.cond = sync.NewCond(&db.mu)

	num := db.ReadCurrentFile()
	if num > 0 {
		v, err := version.Load(dbName, num)
		if err != nil {
			return nil
		}
		db.current = v
	} else {
		db.current = version.New(dbName)
	}

	return &db
}

func (db *Db) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for db.bgCompactionScheduled {
		db.cond.Wait()
	}
}

func (db *Db) Put(key, value []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}

	// TODO: WAL log

	db.mem.Add(seq, internal.TypeValue, key, value)
	return nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	mem := db.mem
	imm := db.imm
	current := db.current
	db.mu.Unlock()

	// first try to find it in memtable
	value, err := mem.Get(key)
	if err != internal.ErrNotFound {
		return value, err
	}

	// then try to find it in immutable
	if imm != nil {
		value, err := imm.Get(key)
		if err != internal.ErrNotFound {
			return value, err
		}
	}

	// finally try to find it in version
	return current.Get(key)
}

func (db *Db) Delete(key []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}
	db.mem.Add(seq, internal.TypeDeletion, key, nil)
	return nil
}
