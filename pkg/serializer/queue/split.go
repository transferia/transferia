package queue

import "github.com/transferia/transferia/pkg/abstract"

func splitByTablePartID(input []abstract.ChangeItem) map[abstract.TablePartID][]abstract.ChangeItem {
	result := make(map[abstract.TablePartID][]abstract.ChangeItem)
	for _, changeItem := range input {
		currTableID := changeItem.TablePartID()
		result[currTableID] = append(result[currTableID], changeItem)
	}
	return result
}
