package storage

import (
	"fmt"
	"os"
)

var (
	DIR_PERMISSION = os.FileMode(0700)
)

func CreateStorageDirIfNotExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		err = os.Mkdir(path, DIR_PERMISSION)
		if err != nil {
			return false, fmt.Errorf("os.Mkdir: %w", err)
		}
	}

	return true, nil
}
