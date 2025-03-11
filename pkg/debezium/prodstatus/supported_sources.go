package prodstatus

import (
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/providers/mysql"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/ydb"
)

var supportedSources = map[string]bool{
	postgres.ProviderType.Name(): true,
	mysql.ProviderType.Name():    true,
	ydb.ProviderType.Name():      true,
}

func IsSupportedSource(src string, _ abstract.TransferType) bool {
	return supportedSources[src]
}
