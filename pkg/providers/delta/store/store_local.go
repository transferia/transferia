//go:build !disable_delta_provider

package store

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/util/iter"
)

var (
	_ Store       = (*Local)(nil)
	_ StoreConfig = (*LocalConfig)(nil)
)

type LocalConfig struct {
	Path string
}

func (s *LocalConfig) isStoreConfig() {}

type Local struct {
	Path string
}

func (l *Local) Root() string { return l.Path }

func (l *Local) Read(path string) (iter.Iter[string], error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFound
		}
		return nil, xerrors.Errorf("local store read: %s:%w", path, err)
	}

	return iter.FromReadCloser(file), nil
}

func (l *Local) ListFrom(path string) (iter.Iter[*FileMeta], error) {
	parent, startFile := filepath.Split(path)
	stats, err := os.ReadDir(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFound
		}
		return nil, xerrors.Errorf("local store list: %s:%w", parent, err)
	}

	stats = yslices.Filter(stats, func(n os.DirEntry) bool {
		return !n.IsDir() && strings.Compare(n.Name(), startFile) >= 0
	})
	res := yslices.Map(stats, func(n os.DirEntry) *FileMeta {
		info, _ := n.Info()
		return &FileMeta{
			path:         filepath.Join(parent, n.Name()),
			timeModified: info.ModTime(),
			size:         uint64(info.Size()),
		}
	})
	sort.Slice(res, func(i, j int) bool {
		return strings.Compare(res[i].path, res[j].path) < 0
	})
	return iter.FromSlice(res...), nil
}

func NewStoreLocal(cfg *LocalConfig) *Local {
	return &Local{
		Path: cfg.Path,
	}
}
