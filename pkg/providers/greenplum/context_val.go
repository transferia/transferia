//go:build !disable_greenplum_provider

package greenplum

type WorkersGpConfigContextKeyStruct struct{}

var WorkersGpConfigContextKey = &WorkersGpConfigContextKeyStruct{}
