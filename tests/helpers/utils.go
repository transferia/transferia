package helpers

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/connection"
	xslices "golang.org/x/exp/slices"
)

var TransferID = "dtt"

func EmptyRegistry() core_metrics.Registry {
	return solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()})
}

func GetEnvOfFail(t *testing.T, key string) string {
	res, ok := os.LookupEnv(key)
	if !ok {
		t.Fail()
	}
	return res
}

func TableMapFromItems(items []abstract.ChangeItem) abstract.TableMap {
	tables := make(abstract.TableMap)
	for _, item := range items {
		if _, ok := tables[item.TableID()]; ok {
			continue
		}
		tables[item.TableID()] = abstract.TableInfo{
			EtaRow: 1,
			IsView: false,
			Schema: item.TableSchema,
		}
	}
	return tables
}

func GetIntFromEnv(varName string) int {
	val, err := strconv.Atoi(os.Getenv(varName))
	if err != nil {
		panic(err)
	}
	return val
}

// StrictEquality - default callback for checksum - just compare typeNames
func StrictEquality(l, r string) bool {
	return l == r
}

func InitConnectionResolver(connections map[string]connection.ManagedConnection) {
	stubResolver := connection.NewStubConnectionResolver()
	var err error
	for connID, conn := range connections {
		err = stubResolver.Add(connID, conn)
		if err != nil {
			panic(err)
		}
	}
	connection.Init(stubResolver)
}

// GetPortFromStr - works when the port is in the end of the string, preceded by a colon
func GetPortFromStr(s string) (int, error) {
	tokens := strings.Split(s, ":")
	if tokens[0] == s {
		return 1, xerrors.Errorf("Unable to find port in string %v (no colon)", s)
	}
	portStr := tokens[len(tokens)-1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 1, xerrors.Errorf("Unable to get port from string %v (unable to parse %v)", s, portStr)
	}
	return port, nil
}

// RemoveColumnsFromChangeItem removes ColumnNames[i] and ColumnValues[i] where ColumnNames[i] is in columnsToRemove.
func RemoveColumnsFromChangeItem(item abstract.ChangeItem, columnsToRemove []string) abstract.ChangeItem {
	res := item
	res.ColumnNames = nil
	res.ColumnValues = nil
	for i, colName := range item.ColumnNames {
		if !xslices.Contains(columnsToRemove, colName) {
			res.ColumnNames = append(res.ColumnNames, colName)
			res.ColumnValues = append(res.ColumnValues, item.ColumnValues[i])
		}
	}
	return res
}
