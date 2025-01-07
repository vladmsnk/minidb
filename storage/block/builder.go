package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type BlockBuilder struct {
	Data       []byte
	Offsets    []uint16
	TargetSize uint16
}

func NewBlockBuilder(targetSize uint16) *BlockBuilder {
	return &BlockBuilder{
		TargetSize: targetSize,
	}
}

func (b *BlockBuilder) estimatedCurrentBlockSize() uint16 {
	return SIZEOF_UINT16 + uint16(len(b.Data)+len(b.Offsets)*int(SIZEOF_UINT16))
}

func (b *BlockBuilder) isEmpty() bool {
	return len(b.Offsets) == 0
}

func (b *BlockBuilder) Add(key, value []byte) bool {
	if len(key) == 0 {
		panic("key is empty")
	}

	requiredSize := uint16(len(key)+len(value)) + 3*SIZEOF_UINT16
	if b.estimatedCurrentBlockSize()+requiredSize > b.TargetSize && !b.isEmpty() {
		return false
	}
	b.Offsets = append(b.Offsets, uint16(len(b.Data)))

	buf := bytes.NewBuffer(b.Data)

	if err := binary.Write(buf, binary.BigEndian, uint16(len(key))); err != nil {
		panic(fmt.Sprintf("failed to write key length: %v", err))
	}

	buf.Write(key)

	if err := binary.Write(buf, binary.BigEndian, uint16(len(value))); err != nil {
		panic(fmt.Sprintf("failed to write value length: %v", err))
	}

	buf.Write(value)

	b.Data = buf.Bytes()

	return true
}

func (b *BlockBuilder) Build() Block {
	if b.isEmpty() {
		return Block{}
	}

	return Block{
		data:    b.Data,
		offsets: b.Offsets,
	}
}
