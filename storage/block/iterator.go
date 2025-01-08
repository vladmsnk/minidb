package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Iterator is an iterator for a single block
type Iterator struct {
	blockData    []byte
	idx          int
	currentKey   []byte
	currentValue []byte
	block        *Block
	err          error
}

// SetBlock sets new block for the iterator
func (i *Iterator) SetBlock(block *Block) {
	i.idx = 0
	i.block = block
	i.blockData = append([]byte(nil), block.data...)
}

func (i *Iterator) decodeEntry() {
	if i.idx >= len(i.block.offsets) {
		i.err = io.EOF
		return
	}

	dataOffset := i.block.offsets[i.idx]
	if int(dataOffset) >= len(i.blockData) {
		i.err = fmt.Errorf("invalid data offset: %d", dataOffset)
		return
	}

	data := i.blockData[dataOffset:]

	if len(data) < 2 {
		i.err = fmt.Errorf("insufficient data for key length")
		return
	}
	keyLen := binary.BigEndian.Uint16(data)
	data = data[2:]

	if len(data) < int(keyLen) {
		i.err = fmt.Errorf("insufficient data for key")
		return
	}
	i.currentKey = data[:keyLen]
	data = data[keyLen:]

	if len(data) < 2 {
		i.err = fmt.Errorf("insufficient data for value length")
		return
	}
	valueLen := binary.BigEndian.Uint16(data)
	data = data[2:]

	if len(data) < int(valueLen) {
		i.err = fmt.Errorf("insufficient data for value")
		return
	}
	i.currentValue = data[:valueLen]
	i.err = nil
}

func (i *Iterator) Valid() bool {
	return i.err == nil
}

func (i *Iterator) Seek(key []byte) {
	i.idx = 0
	for i.idx < len(i.block.offsets) {
		i.decodeEntry()
		if i.err != nil {
			panic(fmt.Sprintf("unexpected error: %v", i.err))
		}

		if bytes.Equal(i.currentKey, key) {
			return
		}
		i.idx++
	}
}

func (i *Iterator) Next() {
	if i.err != nil {
		return
	}
	i.idx++
	i.decodeEntry()
}

func (i *Iterator) GetKey() []byte {
	return i.currentKey
}

func (i *Iterator) GetValue() []byte {
	return i.currentValue
}

func (i *Iterator) Close() {
	i.block = nil
	i.blockData = nil
	i.currentKey = nil
	i.currentValue = nil
	i.err = nil
}
