//go:build !cgo || !oracle
// +build !cgo !oracle

package log_miner

func IsContainsError(err error, skipCodes []string) bool {
	return false
}
