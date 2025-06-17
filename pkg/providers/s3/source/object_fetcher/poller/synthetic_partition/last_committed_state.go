//go:build !disable_s3_provider

package synthetic_partition

import (
	"encoding/json"
	"sort"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
	"github.com/transferia/transferia/pkg/util/set"
)

type lastCommittedState struct {
	ns    int64
	files *set.Set[string]
}

func (s *lastCommittedState) IsNew(newFile *file.File) bool {
	newNS := newFile.LastModified.UnixNano()
	if newNS > s.ns {
		return true
	} else if newNS < s.ns {
		return false
	} else {
		// when ns equal
		return !s.files.Contains(newFile.FileName)
	}
}

func (s *lastCommittedState) SetNew(newFiles []*file.File) {
	if s.ns != newFiles[0].LastModified.UnixNano() {
		s.ns = newFiles[0].LastModified.UnixNano()
		s.files = set.New[string]()
	}
	for _, currFile := range newFiles {
		s.files.Add(currFile.FileName)
	}
}

func (s *lastCommittedState) FromString(in string) {
	type LastCommittedStateExport struct {
		NS    int64
		Files []string
	}
	var state LastCommittedStateExport
	_ = json.Unmarshal([]byte(in), &state)
	s.ns = state.NS
	s.files = set.New[string](state.Files...)
}

func (s *lastCommittedState) ToString() string {
	type LastCommittedStateExport struct {
		NS    int64
		Files []string
	}
	arr := s.files.Slice()
	sort.Strings(arr) // PRETEND NOT TO BE A BOTTLENECK, BCS USUALLY WE WILL HAVE 1 FILE PER 1 NS
	state := LastCommittedStateExport{
		NS:    s.ns,
		Files: arr,
	}
	result, _ := json.Marshal(state)
	return string(result)
}

func (s *lastCommittedState) CalculateNewLastCommittedState(commitNS int64, commitFiles []*file.File) (*lastCommittedState, error) {
	if s.ns == commitNS {
		newFiles := set.New[string](s.files.Slice()...)
		for _, currFile := range commitFiles {
			newFiles.Add(currFile.FileName)
		}
		result, err := newLastCommittedStateStr(s.ns, newFiles.Slice())
		if err != nil {
			return nil, xerrors.Errorf("unable to get last committed state (newLastCommittedStateStr), err: %w", err)
		}
		return result, nil
	} else {
		result, err := newLastCommittedState(commitNS, commitFiles)
		if err != nil {
			return nil, xerrors.Errorf("unable to get last committed state (newLastCommittedState), err: %w", err)
		}
		return result, nil
	}
}

func newLastCommittedStateStr(ns int64, files []string) (*lastCommittedState, error) {
	return &lastCommittedState{
		ns:    ns,
		files: set.New[string](files...),
	}, nil
}

func newLastCommittedState(ns int64, files []*file.File) (*lastCommittedState, error) {
	for _, currFile := range files {
		if currFile.LastModified.UnixNano() != ns {
			return nil, xerrors.New("in newLastCommittedState every 'file.LastModified' should be equal to 'ns'")
		}
	}

	filesArr := make([]string, 0, len(files))
	for _, currFile := range files {
		filesArr = append(filesArr, currFile.FileName)
	}
	return newLastCommittedStateStr(ns, filesArr)
}
