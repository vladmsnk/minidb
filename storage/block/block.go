package block

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

const (
	SIZEOF_UINT16 = uint16(unsafe.Sizeof(uint16(0)))
)

type Block struct {
	data    []byte
	offsets []uint16
}

func (b *Block) Encode() []byte {
	buf := bytes.NewBuffer(b.data)

	for _, offset := range b.offsets {

		// Write 2 bytes for every offset
		binary.Write(buf, binary.BigEndian, offset)
	}

	// Write 2 bytes for the number of offsets
	offsetsLen := uint16(len(b.offsets))
	binary.Write(buf, binary.BigEndian, offsetsLen)

	return buf.Bytes()
}

func Decode(blockBytes []byte) Block {
	block := Block{
		data:    []byte{},
		offsets: []uint16{},
	}

	numOfOffsets := binary.BigEndian.Uint16(blockBytes[len(blockBytes)-2:])

	dataLen := len(blockBytes) - 2 - int(numOfOffsets)*2

	// Extract the data section
	block.data = blockBytes[:dataLen]

	offsetsRaw := blockBytes[dataLen : len(blockBytes)-2]
	block.offsets = make([]uint16, numOfOffsets)
	for i := 0; i < int(numOfOffsets); i++ {
		block.offsets[i] = binary.BigEndian.Uint16(offsetsRaw[i*2:])
	}

	return block
}

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

	if b.estimatedCurrentBlockSize()+uint16(len(key)+len(value))+3*SIZEOF_UINT16 > b.TargetSize && !b.isEmpty() {
		return false
	}

	b.Offsets = append(b.Offsets, uint16(len(b.Data)))

	b.Data = append(b.Data, byte(len(key)>>8), byte(len(key))) // 2 bytes for key length
	b.Data = append(b.Data, key...)

	b.Data = append(b.Data, byte(len(value)>>8), byte(len(value))) // 2 bytes for value length
	b.Data = append(b.Data, value...)

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
