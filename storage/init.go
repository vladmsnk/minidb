package storage

import (
	"fmt"
)

func New(options Options) (*Storage, error) {
	options, err := CheckOptions(options)
	if err != nil {
		return nil, fmt.Errorf("CheckOptions: %w", err)
	}

	exists, err := CreateStorageDirIfNotExists(options.DirPath)
	if err != nil {
		return nil, fmt.Errorf("CreateStorageDirIfNotExists: %w", err)
	}

	var fileNumber int
	if exists {
		// TODO: get the last file number
	}

	logManager, err := NewLogManager(options.DirPath, fileNumber)
	if err != nil {
		return nil, fmt.Errorf("NewLogManager: %w", err)
	}

	index := NewIndex()

	return &Storage{logManager: logManager, compactor: &Compactor{}, index: index}, nil
}
