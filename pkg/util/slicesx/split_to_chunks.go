package slicesx

// SplitToChunks splits `slice` to `n` chunks of `math.Ceil(len(slice)/n)` length.
// Tailing chunks could be shorter (even `nil` if n > len(slice)/2). Also, just `nil` is returned if n < 0.
// Values of result slice are not copied and could be changed through original `slice`.
func SplitToChunks[T any](slice []T, n int) [][]T {
	if n < 0 {
		return nil
	}
	if n == 0 {
		return make([][]T, 0)
	}
	chunkSize := (len(slice) + n - 1) / n
	res := make([][]T, n)
	left, resI := 0, 0
	for left < len(slice) {
		right := min(left+chunkSize, len(slice))
		if resI == n-1 {
			right = len(slice)
		}
		res[resI] = slice[left:right]
		resI++
		left = right
	}
	return res
}
