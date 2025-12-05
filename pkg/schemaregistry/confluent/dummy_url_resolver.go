//go:build !arcadia
// +build !arcadia

package confluent

import (
	"errors"
)

func ResolveYSRNamespaceIDToConnectionParams(namespaceID string) (params YSRConnectionParams, err error) {
	return params, errors.New("not implemented for open-source")
}
