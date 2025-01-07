package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type BlockMeta struct {
	Offset   uint16
	FirstKey []byte
	LastKey  []byte
}

func EncodeBlocksMeta(blocksMeta []BlockMeta) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})

	binary.Write(buf, binary.BigEndian, uint32(len(blocksMeta)))

	for _, blockMeta := range blocksMeta {
		binary.Write(buf, binary.BigEndian, uint32(blockMeta.Offset))

		binary.Write(buf, binary.BigEndian, uint16(len(blockMeta.FirstKey)))
		buf.Write(blockMeta.FirstKey)

		binary.Write(buf, binary.BigEndian, uint16(len(blockMeta.LastKey)))
		buf.Write(blockMeta.LastKey)
	}

	return buf.Bytes(), nil
}

func (b *BlockMeta) Decode(encodedBlocksMeta []byte) ([]BlockMeta, error) {
	buf := bytes.NewBuffer(encodedBlocksMeta)

	var numBlocks uint32
	if err := binary.Read(buf, binary.BigEndian, &numBlocks); err != nil {
		return nil, fmt.Errorf("failed to read the number of blocks: %w", err)
	}

	blocksMeta := make([]BlockMeta, 0, numBlocks)

	for i := 0; i < int(numBlocks); i++ {
		var offset uint32
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return nil, fmt.Errorf("failed to read block offset: %w", err)
		}

		var firstKeyLen uint16
		if err := binary.Read(buf, binary.BigEndian, &firstKeyLen); err != nil {
			return nil, fmt.Errorf("failed to read FirstKey length: %w", err)
		}

		firstKey := make([]byte, firstKeyLen)
		if _, err := buf.Read(firstKey); err != nil {
			return nil, fmt.Errorf("failed to read FirstKey: %w", err)
		}

		var lastKeyLen uint16
		if err := binary.Read(buf, binary.BigEndian, &lastKeyLen); err != nil {
			return nil, fmt.Errorf("failed to read LastKey length: %w", err)
		}

		lastKey := make([]byte, lastKeyLen)
		if _, err := buf.Read(lastKey); err != nil {
			return nil, fmt.Errorf("failed to read LastKey: %w", err)
		}

		blocksMeta = append(blocksMeta, BlockMeta{
			Offset:   uint16(offset),
			FirstKey: firstKey,
			LastKey:  lastKey,
		})
	}

	return blocksMeta, nil
}
