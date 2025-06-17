//go:build !disable_ydb_provider

package ydb

import "github.com/transferia/transferia/pkg/abstract"

func (s *Storage) TableSizeInBytes(table abstract.TableID) (uint64, error) {
	// we force full load for checksum
	return 0, nil
}

func (s *Storage) LoadTopBottomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	// TODO implement me
	panic("implement me")
}

func (s *Storage) LoadRandomSample(table abstract.TableDescription, pusher abstract.Pusher) error {
	// TODO implement me
	panic("implement me")
}

func (s *Storage) LoadSampleBySet(table abstract.TableDescription, keySet []map[string]interface{}, pusher abstract.Pusher) error {
	// TODO implement me
	panic("implement me")
}

func (s *Storage) TableAccessible(table abstract.TableDescription) bool {
	// TODO implement me
	panic("implement me")
}
