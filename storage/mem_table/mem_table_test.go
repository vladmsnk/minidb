package mem_table

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSet(t *testing.T) {
	memTable := NewMemTable(0)

	key1 := "key1"
	data1 := "data1"
	c1 := ValueStruct{Key: key1, Data: []byte(data1)}

	key2 := "key2"
	data2 := "data2"
	c2 := ValueStruct{Key: key2, Data: []byte(data2)}

	t.Run("test 1", func(t *testing.T) {
		memTable.Set(c1)
		memTable.Set(c2)

		value1, _ := memTable.Get(c1.Key)
		value2, _ := memTable.Get(c2.Key)
		assert.Equal(t, c1, value1)
		assert.Equal(t, c2, value2)
	})
}
