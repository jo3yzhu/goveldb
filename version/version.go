package version

import (
	"encoding/binary"
	"github.com/jo3yzhu/goveldb/internal"
	"io"
	"log"
	"os"
	"sort"
)

type FileMetaData struct {
	allowSeeks uint64 // allowed seek missing times before compaction
	number     uint64 // file number, indicate the file name
	fileSize   uint64
	smallest   *internal.InternalKey // indicate the key range of the file
	largest    *internal.InternalKey // indicate the key range of the file
}

// @description: encode a file meta data to a writer
// @param: the writer
// @return: error if any

func (meta *FileMetaData) EncodeTo(w io.Writer) error {
	var err error
	write := func(data interface{}) {
		if err != nil {
			return
		}
		err = binary.Write(w, binary.LittleEndian, data)
	}

	write(meta.allowSeeks)
	write(meta.fileSize)
	write(meta.number)
	if err != nil {
		return err
	}

	err = meta.smallest.EncodeTo(w)
	if err != nil {
		return err
	}

	err = meta.largest.EncodeTo(w)
	if err != nil {
		return err
	}

	return nil
}

// @description: decode a file meta data from a reader
// @param: the reader
// @return: error if any

func (meta *FileMetaData) DecodeFrom(r io.Reader) error {
	var err error
	read := func(data interface{}) {
		if err != nil {
			return
		}
		err = binary.Read(r, binary.LittleEndian, data)
	}

	read(&meta.allowSeeks)
	read(&meta.fileSize)
	read(&meta.number)
	if err != nil {
		return err
	}

	meta.smallest = new(internal.InternalKey)
	err = meta.smallest.DecodeFrom(r)
	if err != nil {
		return err
	}

	meta.largest = new(internal.InternalKey)
	err = meta.largest.DecodeFrom(r)
	return err
}

// Version contains a set of sstable file in each level
// In LevelDB, MVCC is implemented by version set, which is not implemented here

type Version struct {
	tableCache     *TableCache // lru cache of sstable files
	nextFileNumber uint64
	seq            uint64
	files          [internal.NumLevels][]*FileMetaData // file meta data in each level
	compactPointer [internal.NumLevels]*internal.InternalKey // pre-level key at which the next compaction at that level should start
}

// @description: encode a version to a writer
// @param: the writer
// @return: error if any

func (v *Version) EncodeTo(w io.Writer) error {
	var err error
	write := func(data interface{}) {
		if err != nil {
			return
		}
		err = binary.Write(w, binary.LittleEndian, data)
	}

	// first encode next file number and seq
	write(v.nextFileNumber)
	write(v.seq)

	// write num of files and write file meta data of each file
	for level := 0; level < internal.NumLevels; level++ {
		numFiles := len(v.files[level])
		write(int32(numFiles))

		for i := 0; i < numFiles; i++ {
			if err = v.files[level][i].EncodeTo(w); err != nil {
				return err
			}
		}
	}

	return nil
}

// @description: decode a version from a reader
// @param: the reader
// @return: error if any

func (v *Version) DecodeFrom(r io.Reader) error {
	var err error
	read := func(data interface{}) {
		if err != nil {
			return
		}
		err = binary.Read(r, binary.LittleEndian, data)
	}

	read(&v.nextFileNumber)
	read(&v.seq)

	for level := 0; level < internal.NumLevels; level++ {
		var numFiles int32
		read(&numFiles)
		v.files[level] = make([]*FileMetaData, numFiles)

		for i := 0; i < int(numFiles); i++ {
			var meta FileMetaData
			err = meta.DecodeFrom(r)
			if err != nil {
				return err
			}
			v.files[level][i] = &meta
		}
	}

	return nil
}

func New(dbName string) *Version {
	return &Version{
		tableCache:     NewTableCache(dbName),
		nextFileNumber: 1,
	}
}

// @description: load a version from a file
// @param: the database name and file number
// @return: version and error if any

func Load(dbName string, number uint64) (*Version, error) {
	// generate file name by file file number
	// file name formats like "DBName/MANIFEST-000123"
	fileName := internal.DescriptorFileName(dbName, number)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	v := New(dbName)
	err = v.DecodeFrom(file)
	return v, err
}

// @description: save meta file of version to manifest file in disk
// @return: next file number and error

func (v *Version) Save() (uint64, error) {
	tmp := v.nextFileNumber
	fileName := internal.DescriptorFileName(v.tableCache.dbName, v.nextFileNumber)

	v.nextFileNumber++
	file, err := os.Create(fileName)
	if err != nil {
		return tmp, err
	}
	defer file.Close()
	return tmp, v.EncodeTo(file)
}

// @description: log file info in each level of version

func (v *Version) Log() {
	for level := 0; level < internal.NumLevels; level++ {
		for i := 0; i < len(v.files[level]); i++ {
			log.Printf("version[%d]: %d", level, v.files[level][i].number)
		}
	}
}

// @description: deep copy a version
// @return: pointer of copied new version

func (v *Version) Copy() *Version {
	var c Version

	c.tableCache = v.tableCache
	c.nextFileNumber = v.nextFileNumber
	c.seq = v.seq
	for level := 0; level < internal.NumLevels; level++ {
		c.files[level] = make([]*FileMetaData, len(v.files[level]))
		copy(c.files[level], v.files[level]) // deep copy
	}
	return &c
}

func (v *Version) NextSeq() uint64 {
	v.seq++
	return v.seq
}

func (v *Version) NumLevelFiles(l int) int {
	return len(v.files[l])
}

// @description: get key-value from version with binary search
// @param: the target key
// @return: the value and error if any

func (v *Version) Get(key []byte) ([]byte, error) {
	var tmp []*FileMetaData
	var files []*FileMetaData

	// search for target key from level0 to level6, for data is newer in younger level
	// file with larger number is newer
	for level := 0; level < internal.NumLevels; level++ {
		numFiles := len(v.files[level])
		if numFiles == 0 {
			continue // current level is empty, go to next level
		}

		// files in level0 is allowed to overlap each other, every file may contain target key
		// so each file needs to be examined
		if level == 0 {
			for i := 0; i < numFiles; i++ {
				f := v.files[level][i]
				if internal.UserKeyComparator(key, f.smallest.UserKey) >= 0 && internal.UserKeyComparator(key, f.largest.UserKey) <= 0 {
					tmp = append(tmp, f)
				}

				// if there's no matched file in level0, go to next level
				if len(tmp) == 0 {
					continue
				}

				// for level0, if expected file more than 1, sort them by file number
				sort.Slice(tmp, func(i, j int) bool {
					return tmp[i].number > tmp[j].number
				})
				numFiles = len(tmp)
				files = tmp
			}
		} else {

			// files in other level is divided in range, so binary search is available here
			// only one file contain target key
			index := v.findFile(v.files[level], key)

			// if current level doesn't contain such range
			if index >= numFiles {
				files = nil
				numFiles = 0
			} else {
				// tmpFile is first file whose largest key larger than target key
				var tmpFiles [1]*FileMetaData
				tmpFiles[0] = v.files[level][index]

				// if the smallest key of tmpFile is also larger than the target key
				if internal.UserKeyComparator(key, tmpFiles[0].smallest.UserKey) < 0 {
					files = nil
					numFiles = 0
				} else {
					files = tmpFiles[:]
					numFiles = 1
				}
			}
		}

		// search in every possible and put them in table cache
		for i := 0; i < numFiles; i++ {
			f := files[i]
			value, err := v.tableCache.Get(f.number, key)
			if err != internal.ErrNotFound {
				return value, err
			}
		}
	}

	return nil, internal.ErrNotFound
}

// @description: find first file whose largest key greater than target key
// @param: the level and target key
// @return: the file index in the input level

func (v *Version) findFile(files []*FileMetaData, key []byte) int {
	left := 0
	right := len(files)

	// search range is [left, right)
	for left < right {
		mid := (left + right) / 2
		f := files[mid]
		if internal.UserKeyComparator(f.largest.UserKey, key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// @description: delete a file meta data in certain level of version
// @param: level and file need to be deleted
// @notice: the number of file number is unique
// TODO: if level is ordered, binary search is available

func (v *Version) deleteFile(level int, meta *FileMetaData) {
	numFiles := len(v.files[level])
	for i := 0; i < numFiles; i++ {
		if v.files[level][i].number == meta.number {
			v.files[level] = append(v.files[level][:i], v.files[level][i+1:]...) // delete a element using append
			log.Printf("deleteFile, level:%d, num:%d", level, meta.number)
			break
		}
	}
}

// @description: add a file meta data in certain level of version
// @param: level and file need to be added
// @notice: the number of file number is unique

func (v *Version) addFile(level int, meta *FileMetaData) {
	log.Printf("addFile, level:%d, num:%d, %s-%s", level, meta.number, string(meta.smallest.UserKey), string(meta.largest.UserKey))

	if level == 0 {
		// level0 is unordered and maybe overlap each other
		v.files[level] = append(v.files[level], meta)
	} else {
		// ordered and no overlap
		numFiles := len(v.files[level])
		index := v.findFile(v.files[level], meta.smallest.UserKey)

		if index >= numFiles {
			// there's no file whose largest key greater than meta's smallest key
			// then add it in tail
			v.files[level] = append(v.files[level], meta)
		} else {
			var tmp []*FileMetaData
			tmp = append(tmp, v.files[level][:index]...)
			tmp = append(tmp, meta)
			v.files[level] = append(tmp, v.files[level][index:]...)
		}
	}
}

// @description: to find out whether there's an overlap after if a meta file in certain level
// @param: the range of meta file and the level would like to put
// @return: if there's an overlap

func (v *Version) overlapInLevel(level int, smallestKey, largestKey []byte) bool {
	numFiles := len(v.files[level])
	if numFiles == 0 {
		return false
	}

	if level == 0 {
		// level0 maybe overlap each other
		for i := 0; i < numFiles; i++ {
			f := v.files[level][i]
			if internal.UserKeyComparator(smallestKey, f.largest.UserKey) > 0 || internal.UserKeyComparator(f.smallest.UserKey, largestKey) > 0 {
				continue
			} else {
				return true
			}
		}
	} else {
		// no overlap in other level, use binary search
		index := v.findFile(v.files[level], smallestKey)
		if index >= numFiles {
			return false
		} else {
			if internal.UserKeyComparator(largestKey, v.files[level][index].smallest.UserKey) > 0 {
				return true
			}
		}
	}

	return false
}
