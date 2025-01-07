package storage

import (
	"context"
	"encoding/json"
	"fmt"
)

const (
	COMPACT_THRESHOLD_BYTES = 1024 * 1024 // 1MB
)

type Storage struct {
	logManager *LogManager
	compactor  *Compactor
	index      *Index
}

func (s *Storage) Set(ctx context.Context, key string, data string) error {
	c := Command{
		Op:   OP_SET,
		Key:  key,
		Data: data,
	}

	commandInBytes, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}

	location, err := s.logManager.Append(ctx, commandInBytes)
	if err != nil {
		return fmt.Errorf("logManager.Append: %w", err)
	}

	s.index.Set(key, location)

	currentFileSize, err := s.logManager.GetFileSize(ctx)
	if err != nil {
		return fmt.Errorf("logManager.GetFileSize: %w", err)
	}

	if currentFileSize >= COMPACT_THRESHOLD_BYTES {
		s.compactor.Run(ctx)
	}

	return nil
}

func (s *Storage) Get(ctx context.Context, key string) (string, error) {
	location, ok := s.index.Get(key)
	if !ok {
		return "", nil
	}

	data, err := s.logManager.ReadLine(ctx, location)
	if err != nil {
		return "", fmt.Errorf("logManager.ReadLine: %w", err)
	}

	return data, nil
}

func (s *Storage) Remove(ctx context.Context, key string) error {
	c := Command{
		Op:  OP_REMOVE,
		Key: key,
	}

	commandInBytes, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}
	_, err = s.logManager.Append(ctx, commandInBytes)
	if err != nil {
		return fmt.Errorf("logManager.Append: %w", err)
	}

	s.index.Remove(key)

	return nil
}
