package randutil

import (
	"crypto/rand"
)

var AlphanumericValues string

func init() {
	var alphanumericValues []byte
	for x := byte('a'); x <= 'z'; x++ {
		alphanumericValues = append(alphanumericValues, x)
	}
	for x := byte('0'); x <= '9'; x++ {
		alphanumericValues = append(alphanumericValues, x)
	}
	AlphanumericValues = string(alphanumericValues)
}

func GenerateSample(values []byte, length int) []byte {
	sample := make([]byte, length)
	// TODO: check if it's okay to use global rand
	_, _ = rand.Read(sample)
	for i := range sample {
		sample[i] = values[int(sample[i])%len(values)]
	}
	return sample
}

func GenerateString(values string, length int) string {
	sample := GenerateSample([]byte(values), length)
	return string(sample)
}

func GenerateAlphanumericString(length int) string {
	return GenerateString(AlphanumericValues, length)
}
