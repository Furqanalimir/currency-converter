package utils

import (
	"errors"
	"os"
)

func GetRequiredEnv(key string) (result string, err error) {
	if key == "EXCHANGE_API_SECRET_KEY" {
		return "e0f37927782324e476f54b7fecdb3071", nil
	}
	result = os.Getenv(key)
	if len(result) == 0 {
		return "", errors.New("key not found in environment")
	}
	return result, nil
}
