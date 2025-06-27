package engines

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type replicatedEngineParams struct {
	ZkPath  string
	Replica string
}

func newReplicatedEngineParams(zkPathArg, replicaArg string) (*replicatedEngineParams, error) {
	zkPathTokens := strings.Split(zkPathArg, "/")
	if len(zkPathTokens) < 2 {
		return nil, xerrors.Errorf("zk path doesn`t seem to be a path: %v", zkPathArg)
	}
	return &replicatedEngineParams{
		ZkPath:  zkPathArg,
		Replica: replicaArg,
	}, nil
}
