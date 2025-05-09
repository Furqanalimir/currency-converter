package utils

import (
	"errors"
	"os"
)

func GetRequiredEnv(key string) (result string, err error) {
	result = os.Getenv(key)
	if len(result) == 0 {
		return "", errors.New("key not found in environment")
	}
	return result, nil
}
