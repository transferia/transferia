package dataplane

import (
	_ "github.com/transferia/transferia/pkg/providers/airbyte"
	_ "github.com/transferia/transferia/pkg/providers/clickhouse"
	_ "github.com/transferia/transferia/pkg/providers/coralogix"
	_ "github.com/transferia/transferia/pkg/providers/datadog"
	_ "github.com/transferia/transferia/pkg/providers/delta"
	_ "github.com/transferia/transferia/pkg/providers/elastic"
	_ "github.com/transferia/transferia/pkg/providers/eventhub"
	_ "github.com/transferia/transferia/pkg/providers/greenplum"
	_ "github.com/transferia/transferia/pkg/providers/kafka"
	_ "github.com/transferia/transferia/pkg/providers/mongo"
	_ "github.com/transferia/transferia/pkg/providers/mysql"
	_ "github.com/transferia/transferia/pkg/providers/nats"
	_ "github.com/transferia/transferia/pkg/providers/opensearch"
	_ "github.com/transferia/transferia/pkg/providers/postgres"
	_ "github.com/transferia/transferia/pkg/providers/s3/provider"
	_ "github.com/transferia/transferia/pkg/providers/stdout"
	_ "github.com/transferia/transferia/pkg/providers/ydb"
	_ "github.com/transferia/transferia/pkg/providers/yt/init"
)
