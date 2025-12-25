package changeitem

import (
	"sort"
)

func compareColumns(old, new []string) (bool, map[string]int, map[string]int, []string) {
	if len(old) == len(new) {
		diff := false
		for i := range old {
			if old[i] != new[i] {
				diff = true
				break
			}
		}
		if !diff {
			return true, nil, nil, nil
		}
	}
	var total []string
	oldM := map[string]int{}
	newM := map[string]int{}
	for i := range old {
		oldM[old[i]] = i
		total = append(total, old[i])
	}
	for i := range new {
		newM[new[i]] = i
		if _, ok := oldM[new[i]]; !ok {
			total = append(total, new[i])
		}
	}
	return false, oldM, newM, total
}

func InsertsOnly(input []ChangeItem) bool {
	for _, r := range input {
		if r.Kind != InsertKind && r.Kind != SynchronizeKind {
			return false
		}
	}
	return true
}

// Collapse collapses (possible) multiple items in the input into a single (or none) items in the output.
// Currently, it preserves the order of items in the result.
// It should only be applied by sinks which support PRIMARY KEYs. For them the order of items is considered to be of no importance.
func Collapse(input []ChangeItem) []ChangeItem {
	if len(input) < 2 {
		return input
	}
	if InsertsOnly(input) {
		return input
	}

	// to preserve the order
	idxToHashK := map[int]string{}
	hashKToIdx := map[string]int{}

	res := make([]ChangeItem, 0)
	toDelete := map[string]ChangeItem{}
	rows := map[string]ChangeItem{}
	keyCols := input[0].MakeMapKeys()
	if len(keyCols) == 0 {
		return input
	}
	for i, c := range input {
		hashK := c.OldOrCurrentKeysString(keyCols)
		switch c.Kind {
		case InsertKind:
			delete(toDelete, hashK)
			rows[hashK] = c
			hashKToIdx[hashK] = i
			idxToHashK[i] = hashK
		case UpdateKind:
			delete(toDelete, hashK)
			current, ok := rows[hashK]
			if !ok {
				newHashK := c.CurrentKeysString(keyCols)
				rows[newHashK] = c
				hashKToIdx[newHashK] = i
				idxToHashK[i] = newHashK
				continue
			}

			same, oldM, newM, total := compareColumns(current.ColumnNames, c.ColumnNames)
			if same {
				current.ColumnValues = c.ColumnValues
			} else {
				oldValues := current.ColumnValues
				current.ColumnNames = total
				current.ColumnValues = make([]interface{}, len(total))
				for i, col := range total {
					if _, ok := oldM[col]; ok {
						current.ColumnValues[i] = oldValues[oldM[col]]
					}
					if _, ok := newM[col]; ok {
						current.ColumnValues[i] = c.ColumnValues[newM[col]]
					}
				}
			}
			newHashK := current.OldOrCurrentKeysString(keyCols)
			if newHashK != hashK {
				delete(rows, hashK)
			}
			rows[newHashK] = current
			idxToHashK[i] = newHashK
			hashKToIdx[newHashK] = i

		case DeleteKind:
			current, ok := rows[hashK]
			delete(rows, hashK)
			if ok && len(current.OldKeys.KeyValues) > 0 {
				c.OldKeys = current.OldKeys
				hashK = c.OldOrCurrentKeysString(keyCols)
			}
			toDelete[hashK] = c
		default:
			res = append(res, c)
		}
	}
	rowsIdxs := make([]int, 0, len(rows))
	for hashK := range rows {
		rowsIdxs = append(rowsIdxs, hashKToIdx[hashK])
	}
	sort.Ints(rowsIdxs)
	for _, idx := range rowsIdxs {
		res = append(res, rows[idxToHashK[idx]])
	}
	for _, c := range toDelete {
		res = append(res, c)
	}
	return res
}
