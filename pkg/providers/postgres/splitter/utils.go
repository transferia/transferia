//go:build !disable_postgres_provider

package splitter

const (
	mb = uint64(1024) * uint64(1024)
	gb = uint64(1024) * mb
)

func calculatePartCount(totalSize, desiredPartSize, partCountLimit uint64) uint64 {
	partCount := totalSize / desiredPartSize
	if totalSize%desiredPartSize > 0 {
		partCount += 1
	}
	if partCount > partCountLimit {
		partCount = partCountLimit
	}
	return partCount
}
