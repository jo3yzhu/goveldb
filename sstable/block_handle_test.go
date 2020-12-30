package sstable

import (
	"os"
	"reflect"
	"testing"
)

func Test_BlockHandle(t *testing.T) {
	handle1 := BlockHandle{
		Offset: 0,
		Size:   100,
	}

	p := handle1.EncodeToBytes()

	var handle2 BlockHandle
	handle2.DecodeFromBytes(p)

	if reflect.DeepEqual(handle1, handle2) == false {
		t.Fatal("encode decode fail")
	}
}
func Test_Footer(t *testing.T) {
	file, err := os.Create("footer_test")
	if err != nil {
		t.Fatal("file create fail")
	}
	footer1 := Footer{
		MetaIndexHandle: BlockHandle{
			Offset: 0,
			Size:   1,
		},
		IndexHandle: BlockHandle{
			Offset: 2,
			Size:   3,
		},
	}

	err = footer1.EncodeTo(file)
	if err != nil {
		t.Fatal("encode footer fail")
	}

	file.Seek(0,0)

	var footer2 Footer
	err = footer2.DecodeFrom(file)
	if err != nil {
		t.Fatal("decode footer fail")
	}

	if reflect.DeepEqual(footer1, footer2) == false {
		t.Fatal("encode decode fail")
	}
}
