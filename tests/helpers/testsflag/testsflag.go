package testsflag

import (
	"os"
	"testing"
)

var isTest = false

func IsTest() bool {
	return isTest
}

// TurnOff is for cases, when in tests want to turn-off extra validations - for example, in benchmarks OR in checking max log length
func TurnOff() {
	isTest = false
}

// TurnOn is for cases, when we want to return back
func TurnOn() {
	isTest = true
}

func init() {
	isTest = (os.Getenv("YA_TEST_RUNNER") == "1") || testing.Testing()
}
