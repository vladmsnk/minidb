package sstable

import (
	"os"
)

type SsTable struct {
	Id               uint32
	File             *os.File
	BlocksMeta       []BlockMeta
	BlocksMetaOffset uint32
	FirstKey         []byte
	LastKey          []byte
}
