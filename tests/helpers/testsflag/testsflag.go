package testsflag

import (
	"os"
	"testing"
)

var isTest bool = false

func IsTest() bool {
	return isTest
}

// for cases, when in tests want to turn-off extra validations - for example, in benchmarks
func TurnOff() {
	isTest = false
}

func init() {
	isTest = (os.Getenv("YA_TEST_RUNNER") == "1") || testing.Testing()
}
