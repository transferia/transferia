package model

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

type Includeable interface {
	abstract.Includeable

	// FulfilledIncludes returns all include directives which are fulfilled by the given table
	FulfilledIncludes(tID abstract.TableID) []string

	// AllIncludes returns all include directives
	AllIncludes() []string
}

// FilteredTableLister is implemented by storages that can list and filter
// tables in one provider-native call. Providers with hierarchical paths
// (YT, YDB) use this to handle directory-style includes by prefix-matching
// on original paths, avoiding the two-pass TableList + FilterObjects dance
// that relies on TableID-based comparison and cannot represent directories.
type FilteredTableLister interface {
	FilteredTableList(transfer *Transfer) (abstract.TableMap, error)
}

// FilteredMap filters IN-PLACE and returns its first argument
func FilteredMap(m abstract.TableMap, incls ...abstract.Includeable) abstract.TableMap {
TABLES:
	for tID := range m {
		for _, incl := range incls {
			if incl == nil || incl.Include(tID) {
				continue TABLES
			}
			delete(m, tID)
		}
	}
	return m
}

func ExcludeViews(m abstract.TableMap) abstract.TableMap {
	for tID, tInfo := range m {
		if tInfo.IsView {
			delete(m, tID)
		}
	}
	return m
}

func FilteredTableList(storage abstract.Storage, transfer *Transfer) (abstract.TableMap, error) {
	if ftl, ok := storage.(FilteredTableLister); ok {
		return ftl.FilteredTableList(transfer)
	}

	result, err := storage.TableList(transfer)
	if err != nil {
		return nil, xerrors.Errorf("failed to list tables in source: %w", err)
	}
	if incl, ok := transfer.Src.(Includeable); ok {
		result = FilteredMap(result, incl)
	}
	if transfer.DataObjects != nil && len(transfer.DataObjects.IncludeObjects) > 0 {
		result, err = transfer.FilterObjects(result)
		if err != nil {
			return nil, xerrors.Errorf("filter failed: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 15*time.Minute)
	defer cancel()
	if expander, ok := storage.(abstract.PartitionExpander); ok {
		result, err = expander.ExpandPartitions(ctx, result)
		if err != nil {
			return nil, xerrors.Errorf("failed to expand partitions: %w", err)
		}
	}
	return result, nil
}
