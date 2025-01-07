package block

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlocks(t *testing.T) {
	block := Block{
		data:    []byte{0, 1, 'a', 0, 2, 'b', 'c'},
		offsets: []uint16{0, 6},
	}

	encoded := block.Encode()
	expectedEncoded := []byte{
		0, 1, // 2 bytes of key length
		'a',  // key data
		0, 2, // 2 bytes of value length
		'b', 'c', // value data
		0, 0, // offset of the start of the first key value pair
		0, 6, // offset of the start of the second key value pair
		0, 2, // number of key value pairs
	}

	require.ElementsMatch(t, expectedEncoded, encoded)

	decoded := Decode(encoded)
	require.Equal(t, block, decoded)
}
