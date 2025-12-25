package mysql

import "github.com/transferia/transferia/pkg/abstract"

type MysqlStorageParams struct {
	ClusterID   string `log:"true"`
	Host        string `log:"true"`
	Port        int    `log:"true"`
	User        string `log:"true"`
	Password    string
	Database    string `log:"true"`
	TLS         bool   `log:"true"`
	CertPEMFile string

	UseFakePrimaryKey   bool   `log:"true"`
	DegreeOfParallelism int    `log:"true"`
	Timezone            string `log:"true"`

	TableFilter        abstract.Includeable `log:"true"`
	PreSteps           *MysqlDumpSteps      `log:"true"`
	ConsistentSnapshot bool                 `log:"true"`
	RootCAFiles        []string
	ConnectionID       string `log:"true"`
}
