package abstract

import (
	"fmt"
	"sort"
	"strings"
)

type OperationTablePart struct {
	OperationID   string
	Schema        string // Table schema or namespace
	Name          string // Table name
	Offset        uint64 // Table part offset
	Filter        string // Table part filter
	PartsCount    uint64 // Parts count for table
	PartIndex     uint64 // Index of this part in the table
	WorkerIndex   *int   // Worker index, that assigned to this part. If nil - worker not assigned yet.
	ETARows       uint64 // How much rows in this part
	CompletedRows uint64 // How much rows already copied
	ReadBytes     uint64 // How many bytes were read from the source
	Completed     bool   // Is this part already copied
}

func NewOperationTablePartFromDescription(operationID string, description *TableDescription) *OperationTablePart {
	return &OperationTablePart{
		OperationID:   operationID,
		Schema:        description.Schema,
		Name:          description.Name,
		Offset:        description.Offset,
		Filter:        string(description.Filter),
		PartsCount:    0,
		PartIndex:     0,
		WorkerIndex:   nil,
		ETARows:       description.EtaRow,
		CompletedRows: 0,
		ReadBytes:     0,
		Completed:     false,
	}
}

func (t *OperationTablePart) Copy() *OperationTablePart {
	return &OperationTablePart{
		OperationID:   t.OperationID,
		Schema:        t.Schema,
		Name:          t.Name,
		Offset:        t.Offset,
		Filter:        t.Filter,
		PartsCount:    t.PartsCount,
		PartIndex:     t.PartIndex,
		WorkerIndex:   t.WorkerIndex,
		ETARows:       t.ETARows,
		CompletedRows: t.CompletedRows,
		ReadBytes:     t.ReadBytes,
		Completed:     t.Completed,
	}
}

func (t *OperationTablePart) CompletedPercent() float64 {
	percent := float64(0)

	if t.Completed {
		// all rows in part copied, since Eta Rows might be an estimation they might be slightly off we correct eta with actual rows
		if t.ETARows != t.CompletedRows {
			t.ETARows = t.CompletedRows // we get 100%
		}
	}

	if t.ETARows != 0 {
		percent = (float64(t.CompletedRows) / float64(t.ETARows)) * 100
	}
	return percent
}

func (t *OperationTablePart) ToTableDescription() *TableDescription {
	return &TableDescription{
		Name:   t.Name,
		Schema: t.Schema,
		Filter: WhereStatement(t.Filter),
		EtaRow: t.ETARows,
		Offset: t.Offset,
	}
}

func (t *OperationTablePart) ToTableID() *TableID {
	return &TableID{
		Name:      t.Name,
		Namespace: t.Schema,
	}
}

func (t *OperationTablePart) TableFQTN() string {
	return t.ToTableID().Fqtn()
}

func (t *OperationTablePart) TableKey() string {
	return fmt.Sprintf(
		"OperationID: %v, Name: '%v', Schema: '%v'",
		t.OperationID, t.Name, t.Schema)
}

func (t *OperationTablePart) Key() string {
	return fmt.Sprintf(
		"OperationID: %v, Name: '%v', Schema: '%v', Part: %v, Offset: %v, Filter: '%v'",
		t.OperationID, t.Name, t.Schema, t.PartIndex, t.Offset, t.Filter)
}

func (t *OperationTablePart) String() string {
	otherInfo := []string{}
	if t.ETARows != 0 {
		otherInfo = append(otherInfo, fmt.Sprintf("ETARows: %v", t.ETARows))
	}
	if t.Offset != 0 {
		otherInfo = append(otherInfo, fmt.Sprintf("Offset: %v", t.Offset))
	}
	if t.Filter != "" {
		otherInfo = append(otherInfo, fmt.Sprintf("Filter: %v", t.Filter))
	}
	otherInfoString := ""
	if len(otherInfo) > 0 {
		otherInfoString = fmt.Sprintf(" (%v)", strings.Join(otherInfo, ", "))
	}
	return fmt.Sprintf("%v [%v/%v]%v", t.TableFQTN(), t.PartIndex+1, t.PartsCount, otherInfoString)
}

func (t *OperationTablePart) StringWithoutFilter() string {
	otherInfo := []string{}
	if t.ETARows != 0 {
		otherInfo = append(otherInfo, fmt.Sprintf("ETARows: %v", t.ETARows))
	}
	if t.Offset != 0 {
		otherInfo = append(otherInfo, fmt.Sprintf("Offset: %v", t.Offset))
	}
	otherInfoString := ""
	if len(otherInfo) > 0 {
		otherInfoString = fmt.Sprintf(" (%v)", strings.Join(otherInfo, ", "))
	}
	return fmt.Sprintf("%v [%v/%v]%v", t.TableFQTN(), t.PartIndex+1, t.PartsCount, otherInfoString)
}

func (t *OperationTablePart) Sharded() bool {
	return t.PartsCount > 1
}

func SortTableParts(parts []*OperationTablePart) {
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].TableKey() != parts[j].TableKey() {
			return parts[i].TableKey() < parts[j].TableKey()
		}
		if parts[i].Offset != parts[j].Offset {
			return parts[i].Offset < parts[j].Offset
		}
		return parts[i].Filter < parts[j].Filter
	})
}

func SortAndDeduplicateTableParts(parts []*OperationTablePart) []*OperationTablePart {
	partsMap := map[string]*OperationTablePart{}
	for _, incomingPart := range parts {
		key := fmt.Sprintf("%s-%s-%v", incomingPart.TableKey(), incomingPart.Filter, incomingPart.Offset)
		if old, ok := partsMap[key]; ok && (incomingPart.CompletedRows <= old.CompletedRows &&
			incomingPart.ReadBytes <= old.ReadBytes &&
			(old.Completed || incomingPart.Completed == old.Completed)) {
			continue
		}
		partsMap[key] = incomingPart
	}

	newParts := []*OperationTablePart{}
	for _, part := range partsMap {
		newParts = append(newParts, part)
	}

	SortTableParts(newParts)
	return newParts
}
