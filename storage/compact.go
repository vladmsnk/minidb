package storage

import "context"

type Compactor struct {
}

func (c *Compactor) Run(ctx context.Context) error {
	go func() {
		// TODO: Implement compaction
	}()

	return nil
}
