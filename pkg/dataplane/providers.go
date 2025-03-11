package dataplane

import (
	_ "github.com/transferria/transferria/pkg/providers/airbyte"
	_ "github.com/transferria/transferria/pkg/providers/clickhouse"
	_ "github.com/transferria/transferria/pkg/providers/coralogix"
	_ "github.com/transferria/transferria/pkg/providers/datadog"
	_ "github.com/transferria/transferria/pkg/providers/delta"
	_ "github.com/transferria/transferria/pkg/providers/elastic"
	_ "github.com/transferria/transferria/pkg/providers/eventhub"
	_ "github.com/transferria/transferria/pkg/providers/greenplum"
	_ "github.com/transferria/transferria/pkg/providers/kafka"
	_ "github.com/transferria/transferria/pkg/providers/mongo"
	_ "github.com/transferria/transferria/pkg/providers/mysql"
	_ "github.com/transferria/transferria/pkg/providers/opensearch"
	_ "github.com/transferria/transferria/pkg/providers/postgres"
	_ "github.com/transferria/transferria/pkg/providers/s3/provider"
	_ "github.com/transferria/transferria/pkg/providers/stdout"
	_ "github.com/transferria/transferria/pkg/providers/ydb"
	_ "github.com/transferria/transferria/pkg/providers/yt/init"
)
