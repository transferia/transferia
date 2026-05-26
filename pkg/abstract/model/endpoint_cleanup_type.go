package model

import (
	"fmt"
)

type CleanupType string

const (
	Drop            CleanupType = "Drop"
	Truncate        CleanupType = "Truncate"
	DisabledCleanup CleanupType = "Disabled"
	Replace         CleanupType = "Replace"
)

func (ct CleanupType) IsValid() error {
	switch ct {
	case Drop, Truncate, DisabledCleanup, Replace:
		return nil
	}
	return fmt.Errorf("invalid cleanup type: %v", ct)
}

const TmpTableSuffix = "tmp"

func MakeTmpTableName(tableName, transferId, suffix string) string {
	return fmt.Sprintf("%s_%s_%s", tableName, transferId, suffix)
}

func MakeTmpSuffix(transferId, suffix string) string {
	return fmt.Sprintf("_%s_%s", transferId, suffix)
}
