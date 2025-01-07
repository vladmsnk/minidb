package storage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
)

type LogManager struct {
	file              *os.File
	currentFileNumber int
}

func NewLogManager(dirPath string, fileNumber int) (*LogManager, error) {
	filePath := fmt.Sprintf("%s/log-%d", dirPath, fileNumber)

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("os.OpenFile: %w", err)
	}

	return &LogManager{file: f}, nil
}

func (l *LogManager) GetFileSize(_ context.Context) (int64, error) {
	stat, err := l.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("file.Stat: %w", err)
	}

	return stat.Size(), nil
}

func (l *LogManager) Append(_ context.Context, data []byte) (int64, error) {

	data = append(data, '\n')

	offset, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("file.Seek: %w", err)
	}

	_, err = l.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("file.Write: %w", err)
	}

	err = l.file.Sync()
	if err != nil {
		return 0, fmt.Errorf("file.Sync: %w", err)
	}

	return offset, nil
}

func (l *LogManager) ReadLine(_ context.Context, offset int64) (string, error) {

	_, err := l.file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("file.Seek: %w", err)
	}

	reader := bufio.NewReader(l.file)

	data, err := reader.ReadString("\n"[0])
	if err != nil {
		return "", fmt.Errorf("reader.ReadString: %w", err)
	}

	// Remove the newline character.
	if len(data) > 0 {
		data = data[:len(data)-1]
	}

	return data, nil
}
