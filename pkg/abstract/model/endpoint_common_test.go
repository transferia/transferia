package model

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func checkEncodeDecode(item interface{}) error {
	mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	err := encCache.Encode(item)
	if err != nil {
		return err
	}

	pCache := bytes.NewBuffer(mCache.Bytes())
	decCache := gob.NewDecoder(pCache)
	switch t := item.(type) {
	case *TransferOperation:
		var decoded TransferOperation
		return decCache.Decode(&decoded)
	default:
		return xerrors.Errorf("unknown type: %v", fmt.Sprintf("%T", t))
	}
}

func TestEndpoints_GobEncode(t *testing.T) {
	require.NoError(t, checkEncodeDecode(&TransferOperation{}))
	require.NoError(t, checkEncodeDecode(&TransferOperation{TaskType: abstract.TaskType{Task: abstract.Activate{}}}))
}
