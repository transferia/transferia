package util

import (
	"errors"
	"os"
)

func IsFileExist(file string) (bool, error) {
	if _, err := os.Stat(file); err == nil {
		// path/to/whatever exists
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		// path/to/whatever does *not* exist
		return false, nil
	} else {
		return false, err
	}
}
