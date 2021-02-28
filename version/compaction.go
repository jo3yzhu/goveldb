package version

import (
	"github.com/jo3yzhu/goveldb/internal"
	"github.com/jo3yzhu/goveldb/memtable"
	"github.com/jo3yzhu/goveldb/sstable"
	"log"
)

type Compaction struct {
	level  int
	inputs [2][]*FileMetaData
}

// @description: tell that whether a compaction instance can be simplified
// @note: if a compaction is as trivial as code below, sstable files in inputs[0] can be direction moved to next level

func (c *Compaction) isTrivialMove() bool {
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0
}

func (c *Compaction) Log() {
	log.Printf("Compaction, level:%d", c.level)
	for i := 0; i < len(c.inputs[0]); i++ {
		log.Printf("inputs[0]: %d", c.inputs[0][i].number)
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		log.Printf("inputs[1]: %d", c.inputs[1][i].number)
	}
}

// @description: write the immutable to level0 into this version, which is known as minor compaction
// @param: the memtable needed to be written

func (v *Version) WriteLevel0Table(imm *memtable.MemTable) {

	// generate sstable builder for writing sstable
	var meta FileMetaData
	meta.allowSeeks = 1 << 30
	meta.number = v.nextFileNumber
	v.nextFileNumber++
	builder := sstable.NewTableBuilder(internal.TableFileName(v.tableCache.dbName, meta.number))

	// iterate memtable
	iter := imm.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		meta.smallest = iter.InternalKey()
		for ; iter.Valid(); iter.Next() {
			meta.largest = iter.InternalKey()
			builder.Add(iter.InternalKey())
		}
		_ = builder.Finish()
		meta.fileSize = uint64(builder.FileSize())

		// is this necessary?
		meta.largest.UserValue = nil
		meta.smallest.UserValue = nil
	}

	// pick a level for writing
	level := 0
	if !v.overlapInLevel(0, meta.smallest.UserKey, meta.largest.UserKey) {
		// find the most deep level where's no overlap after inserting meta
		for ; level < internal.MaxMemCompactLevel; level++ {
			if v.overlapInLevel(level+1, meta.smallest.UserKey, meta.largest.UserKey) {
				break
			}
		}
	}

	v.addFile(level, &meta)
}

// @description: calculate total file size of a level and then choose one to compact
// @param: the files of level

func totalFileSize(files []*FileMetaData) uint64 {
	var sum uint64
	for i := 0; i < len(files); i++ {
		sum += files[i].fileSize
	}
	return sum
}

// @description: return the size threshold of each level before compacting
// @note: result for level0 is not used because we compact level0 by numbers of its files

func maxBytesForLevel(level int) float64 {
	result := 10 * 1048576.0
	for level > 1 {
		result *= 10
		level--
	}

	return result
}

// @description: to give which level should be compacted
// @note: level0 is specially treated

func (v *Version) pickCompactionLevel() int {
	compactionLevel := -1
	bestScore := 1.0
	score := 0.0

	// score for each level, level which has highest score would be compacted
	for level := 0; level < internal.NumLevels-1; level++ {
		if level == 0 {
			score = float64(len(v.files[0])) / float64(internal.L0CompactionTrigger)
		} else {
			score = float64(totalFileSize(v.files[level])) / maxBytesForLevel(level)
		}

		if score > bestScore {
			bestScore = score
			compactionLevel = level
		}
	}

	return compactionLevel
}

// @description: pick two level should be compacted later
// @note: level0 is specially treated and process of that is simplified

func (v *Version) pickCompaction() *Compaction {
	var c Compaction
	c.level = v.pickCompactionLevel()
	if c.level < 0 {
		return nil
	}

	var smallest, largest *internal.InternalKey

	// pick sstable files which overlap each other in level0
	// as sstable in level0 may overlap each other, so everyone needs to be examined
	// here I want to keep it simple, so put all file of level0 into c.inputs[0]
	if c.level == 0 {
		c.inputs[0] = append(c.inputs[0], v.files[c.level]...)
		smallest = v.files[0][0].smallest
		largest = v.files[0][0].largest
		for i := 1; i < len(c.inputs[0]); i++ {
			f := c.inputs[0][i]
			if internal.InternalKeyComparator(f.largest, largest) > 0 {
				largest = f.largest
			}
			if internal.InternalKeyComparator(f.smallest, smallest) < 0 {
				smallest = f.smallest
			}
		}
	} else {
		// pick just ONE file in all levels except for level0
		for i := 0; i < len(v.files[c.level]); i++ {
			f := v.files[c.level][i]

			// TODO: compactPointer is not used here, but what does that mean?
			if v.compactPointer[c.level] == nil || internal.InternalKeyComparator(f.largest, v.compactPointer[c.level]) > 0 {
				c.inputs[0] = append(c.inputs[0], f)
				break
			}
		}
		if len(c.inputs[0]) == 0 {
			c.inputs[0] = append(c.inputs[0], v.files[c.level][0])
		}
		smallest = c.inputs[0][0].smallest
		largest = c.inputs[0][0].largest
	}

	// to find out all sstable files in level + 1 which are overlapped with previous level
	// if any, put it into c.inputs[1]
	for i := 0; i < len(v.files[c.level+1]); i++ {
		f := v.files[c.level+1][i]

		// completely before specified range, skip it,
		if internal.InternalKeyComparator(f.largest, smallest) < 0 {
			continue
			// completely after specified range; skip it,
		} else if internal.InternalKeyComparator(f.smallest, largest) > 0 {
			continue
		} else {
			c.inputs[1] = append(c.inputs[1], f)
		}
	}

	return &c
}

func (v *Version) makeInputIterator(c *Compaction) *MergingIterator {
	var list []*sstable.Iterator

	// load iterators of sstable files to be merged into memory and construct a MergingIterator
	for i := 0; i < len(c.inputs[0]); i++ {
		list = append(list, v.tableCache.NewIterator(c.inputs[0][i].number))
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		list = append(list, v.tableCache.NewIterator(c.inputs[1][i].number))
	}
	return NewMergingIterator(list)
}

// @description: compact the inputs sstable file picked by v.pickCompaction
// @note1: new sstable file should be created if newly merged file has reached the limit size of sstable file

func (v *Version) DoCompactionWork() bool {
	c := v.pickCompaction()
	if c == nil {
		return false
	}

	log.Printf("DoCompactionWork begin\n")
	defer log.Printf("DoCompactionWork end\n")
	c.Log()

	if c.isTrivialMove() {
		// just move it to next level
		v.deleteFile(c.level, c.inputs[0][0])
		v.addFile(c.level+1, c.inputs[0][0])
		return true
	}

	var list []*FileMetaData             // newly merged sstable
	var currentKey *internal.InternalKey // to remove duplicated internal key
	iter := v.makeInputIterator(c)

	// begin to create a new merged sstable
	// internal keys of the same user key are sorted by seq in sstable, so for the same user key, the newer one has older seq
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var meta FileMetaData
		meta.allowSeeks = 1 << 30
		meta.number = v.nextFileNumber
		v.nextFileNumber++

		fileName := internal.TableFileName(v.tableCache.dbName, meta.number)
		builder := sstable.NewTableBuilder(fileName)
		meta.smallest = iter.InternalKey()

		for ; iter.Valid(); iter.Next() {
			// bug fix
			if iter.InternalKey().Type == internal.TypeDeletion {
				continue
			}

			if currentKey != nil {
				if iter.InternalKey().Type == internal.TypeDeletion {
					continue
				}
				// the older key input by user may be overwritten, so UserKey comparison is needed instead of InternalKey comparison
				duplicated := internal.UserKeyComparator(iter.InternalKey().UserKey, currentKey.UserKey)
				if duplicated == 0 {
					continue
				} else if duplicated < 0 {
					log.Fatalf("%s < %s", string(iter.InternalKey().UserKey), string(currentKey.UserKey))
				}
			}
			// TODO: maybe fix a bug
			currentKey = iter.InternalKey()
			meta.largest = iter.InternalKey()
			builder.Add(iter.InternalKey())

			// a newly merged file cannot be too large in compaction
			if builder.FileSize() > internal.MaxFileSize {
				break
			}
		}

		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())
		meta.smallest.UserKey = nil
		meta.largest.UserKey = nil

		list = append(list, &meta)
	}

	// the files after merged would be ignored in version instance instead of deleted
	for i := 0; i < len(c.inputs[0]); i++ {
		v.deleteFile(c.level, c.inputs[0][i])
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		v.deleteFile(c.level+1, c.inputs[1][i])
	}

	// add newly merged file to version
	for i := 0; i < len(list); i++ {
		v.addFile(c.level+1, list[i])
	}

	return true
}
