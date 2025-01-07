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
