package version

import (
	"fmt"
	"github.com/jo3yzhu/goveldb/internal"
	"github.com/jo3yzhu/goveldb/memtable"
	"testing"
)

func Test_Version_Get(t *testing.T) {
	v := New("./")
	var f FileMetaData
	f.number = 123
	f.smallest = internal.NewInternalKey(1, internal.TypeValue, []byte("123"), nil)
	f.largest = internal.NewInternalKey(1, internal.TypeValue, []byte("125"), nil)
	v.files[0] = append(v.files[0], &f)

	value, err := v.Get([]byte("123"))
	fmt.Println(err, value)
}

func Test_Version_Load(t *testing.T) {
	v := New("./")
	memTable := memtable.New()
	memTable.Add(1234567, internal.TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	v.WriteLevel0Table(memTable)
	n, _ := v.Save()

	v2, _ := Load("./", n)

	value, err := v2.Get([]byte("aadsa34a"))

	if err != nil {
		t.Fail()
	}
	fmt.Println(err, value)
}