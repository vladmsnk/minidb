package storage

import (
	"sync"

	"minidb/storage/mem_table"
)

type LsmStorageState struct {
	StateLock sync.RWMutex

	// CurrentMemTable is the table that is currently being written to
	CurrentMemTable *mem_table.MemTable

	// ImmutableMemTables are tables that are no longer being written to, but waiting to be compacted
	ImmutableMemTables []*mem_table.MemTable
}

func (l *LsmStorageState) get(key string) (mem_table.ValueStruct, bool, error) {
	tables, err := l.getTables()
	if err != nil {
		return mem_table.ValueStruct{}, false, err
	}

	for _, table := range tables {
		if v, ok := table.Get(key); ok {
			return v, ok, nil
		}
	}

	// todo - implement search in SSTables
	return mem_table.ValueStruct{}, false, nil
}

func (l *LsmStorageState) put(key string, data string) error {
	// believe that memtable is thread-safe
	l.StateLock.RLock()

	l.CurrentMemTable.Set(mem_table.ValueStruct{Key: key, Data: []byte(data)})

	if l.CurrentMemTable.ApproximateSize.Load() >= mem_table.MEMTABLE_MAX_SIZE_BYTES {
		l.StateLock.RUnlock()
		l.freezeCurrentMemTable()
		l.StateLock.Lock()
	}

	l.StateLock.RUnlock()
	return nil
}

// delete does not actually delete the key, but inserts empty data
func (l *LsmStorageState) delete(key string) error {
	l.StateLock.RLock()

	l.CurrentMemTable.Delete(key)

	if l.CurrentMemTable.ApproximateSize.Load() >= mem_table.MEMTABLE_MAX_SIZE_BYTES {
		l.StateLock.RUnlock()
		l.freezeCurrentMemTable()
		l.StateLock.Lock()
	}

	l.StateLock.RUnlock()
	return nil
}

func (l *LsmStorageState) freezeCurrentMemTable() {
	l.StateLock.Lock()

	l.ImmutableMemTables = append(l.ImmutableMemTables, l.CurrentMemTable)
	l.CurrentMemTable = mem_table.NewMemTable(uint(len(l.ImmutableMemTables)))

	l.StateLock.Unlock()
}

func (l *LsmStorageState) getTables() ([]*mem_table.MemTable, error) {
	var tables []*mem_table.MemTable
	l.StateLock.RLock()

	tables = append(tables, l.CurrentMemTable)

	// Reverse the order of ImmutableMemTables
	// to return the most recent tables first
	last := len(l.ImmutableMemTables) - 1
	for i := range l.ImmutableMemTables {
		tables = append(tables, l.ImmutableMemTables[last-i])
	}

	l.StateLock.RUnlock()
	return tables, nil
}
