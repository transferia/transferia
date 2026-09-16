package testenv

import (
	"os"
	"strconv"
	"testing"
)

func GetEnvOfFail(t *testing.T, key string) string {
	res, ok := os.LookupEnv(key)
	if !ok {
		t.Fail()
	}
	return res
}

func GetIntFromEnv(varName string) int {
	val, err := strconv.Atoi(os.Getenv(varName))
	if err != nil {
		panic(err)
	}
	return val
}
