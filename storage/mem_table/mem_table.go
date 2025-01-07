package mem_table

import (
	"github.com/google/btree"
	"sync/atomic"
)

const (
	MEMTABLE_MAX_SIZE_BYTES = 64 << 20 // 64 MB
)

type MemTable struct {
	Id uint

	InMemoryStorage *btree.BTreeG[ValueStruct]
	ApproximateSize atomic.Uint32
}

type ValueStruct struct {
	Key  string
	Data []byte
}

func NewMemTable(id uint) *MemTable {
	return &MemTable{
		InMemoryStorage: btree.NewG[ValueStruct](2, func(a, b ValueStruct) bool {
			return a.Key < b.Key
		}),
		Id: id,
	}
}

func (m *MemTable) Set(command ValueStruct) {
	m.ApproximateSize.Add(uint32(len([]byte(command.Key)) + len(command.Data)))

	m.InMemoryStorage.ReplaceOrInsert(command)
}

func (m *MemTable) Delete(key string) {
	m.ApproximateSize.Add(uint32(len(key)))

	m.InMemoryStorage.ReplaceOrInsert(ValueStruct{Key: key})
}

func (m *MemTable) Get(key string) (ValueStruct, bool) {
	c, ok := m.InMemoryStorage.Get(ValueStruct{Key: key})

	return c, ok
}
