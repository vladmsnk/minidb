package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"minidb/storage/block"
	"os"
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

type SsTable struct {
	Id               uint32
	File             *os.File
	BlocksMeta       []BlockMeta
	BlocksMetaOffset uint32
	FirstKey         []byte
	LastKey          []byte
}

type SsTableBuilder struct {
	// The meta blocks that hold info for data blocks.
	BlocksMeta []BlockMeta
	FirstKey   []byte
	LastKey    []byte

	Data []byte // encoded blocks

	blockSize uint16 // target size of each block

	BlockBuilder *block.BlockBuilder
}

func NewSsTableBuilder(blockSize uint16) *SsTableBuilder {
	return &SsTableBuilder{
		blockSize:    blockSize,
		BlockBuilder: block.NewBlockBuilder(blockSize),
	}

}

func (s *SsTableBuilder) EstimatedSize() uint16 {
	return uint16(len(s.Data))
}

func (s *SsTableBuilder) Add(key []byte, value []byte) {
	if len(s.FirstKey) == 0 {
		s.FirstKey = append([]byte(nil), key...)
		s.LastKey = append([]byte(nil), key...)
	}

	// it is enough space in block for a key-value pair
	if s.BlockBuilder.Add(key, value) {
		s.LastKey = append([]byte(nil), key...)
		return
	}

	s.FinishBlock()

	if !s.BlockBuilder.Add(key, value) {
		panic("key-value pair is too big")
	}

	s.FirstKey = append([]byte(nil), key...)
	s.LastKey = append([]byte(nil), key...)
}

func (s *SsTableBuilder) FinishBlock() {
	currentBlock := s.BlockBuilder.Build()

	s.BlocksMeta = append(s.BlocksMeta, BlockMeta{
		Offset:   uint16(len(s.Data)),
		FirstKey: append([]byte(nil), s.FirstKey...),
		LastKey:  append([]byte(nil), s.LastKey...),
	})

	s.Data = append(s.Data, currentBlock.Encode()...)
	s.BlockBuilder = block.NewBlockBuilder(s.blockSize)
}

func (s *SsTableBuilder) Build(path string) (*SsTable, error) {
	s.FinishBlock()
	buf := bytes.NewBuffer(s.BlockBuilder.Data)

	metaOffset := buf.Len()
	encodedMeta, err := EncodeBlocksMeta(s.BlocksMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to encode block metadata: %w", err)
	}

	buf.Write(encodedMeta)

	if err := binary.Write(buf, binary.BigEndian, uint32(metaOffset)); err != nil {
		return nil, fmt.Errorf("failed to write metadata offset: %w", err)
	}

	// write built sstable to file
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("file.Write: %w", err)
	}

	err = f.Sync()
	if err != nil {
		return nil, fmt.Errorf("file.Sync: %w", err)
	}
	if len(s.BlocksMeta) == 0 {
		panic("no blocks")
	}

	firstKey := append([]byte(nil), s.BlocksMeta[0].FirstKey...)
	lastKey := append([]byte(nil), s.BlocksMeta[len(s.BlocksMeta)-1].LastKey...)

	return &SsTable{
		File:             f,
		BlocksMeta:       s.BlocksMeta,
		BlocksMetaOffset: uint32(metaOffset),
		FirstKey:         firstKey,
		LastKey:          lastKey,
	}, nil
}
