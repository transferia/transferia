package instanceutil

import (
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

// JobIndex calculates an index assigned to the current machine.
func JobIndex() (int, error) {
	instanceName, err := GetGoogleCEMetaData(GoogleName, false)
	if err != nil {
		return 0, xerrors.Errorf("fail to get instance name: %w", err)
	}
	parts := strings.Split(instanceName, "-")
	if len(parts) == 1 {
		return 0, xerrors.Errorf("instance name is in unexpected format: %q", instanceName)
	}
	jobIDX := parts[len(parts)-1]
	idx, err := strconv.Atoi(jobIDX)
	if err != nil {
		return 0, xerrors.Errorf("fail to parse job index from instance name %q: %w", instanceName, err)
	}
	if idx < 1 {
		return 0, nil
	}
	return idx - 1, nil
}
