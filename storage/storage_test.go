package storage

import (
	"context"
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	ctx := context.Background()

	db, err := New(GetDefaultOptions())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Run("test 1", func(t *testing.T) {
		key := "key1"
		data := "data1"

		db.Set(ctx, key, data)

		value, _ := db.Get(ctx, key)
		fmt.Print(value)
	})
}
