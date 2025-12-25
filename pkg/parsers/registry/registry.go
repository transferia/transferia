package registry

import (
	_ "github.com/transferia/transferia/pkg/parsers/registry/audittrailsv1"
	_ "github.com/transferia/transferia/pkg/parsers/registry/blank"
	_ "github.com/transferia/transferia/pkg/parsers/registry/cloudevents"
	_ "github.com/transferia/transferia/pkg/parsers/registry/cloudlogging"
	_ "github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry"
	_ "github.com/transferia/transferia/pkg/parsers/registry/debezium"
	_ "github.com/transferia/transferia/pkg/parsers/registry/json"
	_ "github.com/transferia/transferia/pkg/parsers/registry/native"
	_ "github.com/transferia/transferia/pkg/parsers/registry/protobuf"
	_ "github.com/transferia/transferia/pkg/parsers/registry/raw2table"
	_ "github.com/transferia/transferia/pkg/parsers/registry/tskv"
)
