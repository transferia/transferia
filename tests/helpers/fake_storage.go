package helpers

import (
	"context"

	"github.com/transferria/transferria/pkg/abstract"
)

type FakeStorage struct {
	changeItems []abstract.ChangeItem
}

func (f *FakeStorage) Close()      {}
func (f *FakeStorage) Ping() error { return nil }
func (f *FakeStorage) LoadTable(_ context.Context, _ abstract.TableDescription, pusher abstract.Pusher) error {
	_ = pusher(f.changeItems)
	return nil
}
func (f *FakeStorage) TableSchema(_ context.Context, _ abstract.TableID) (*abstract.TableSchema, error) {
	return nil, nil
}
func (f *FakeStorage) TableList(_ abstract.IncludeTableList) (abstract.TableMap, error) {
	return nil, nil
}
func (f *FakeStorage) ExactTableRowsCount(_ abstract.TableID) (uint64, error)    { return 0, nil }
func (f *FakeStorage) EstimateTableRowsCount(_ abstract.TableID) (uint64, error) { return 0, nil }
func (f *FakeStorage) TableExists(_ abstract.TableID) (bool, error)              { return false, nil }

func NewFakeStorage(changeItems []abstract.ChangeItem) *FakeStorage {
	return &FakeStorage{
		changeItems: changeItems,
	}
}
