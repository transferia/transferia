package common

import (
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
)

type OriginalTypeInfo struct {
	OriginalType string            `json:"original_type"`
	Properties   map[string]string `json:"properties,omitempty"`
}

func (i *OriginalTypeInfo) GetArrElemTypeDescr() *OriginalTypeInfo {
	newOriginalTypeInfo := *i
	newOriginalTypeInfo.OriginalType = pgcommon.GetArrElemTypeDescr(newOriginalTypeInfo.OriginalType)
	return &newOriginalTypeInfo
}
