package registry

import (
	_ "github.com/transferria/transferria/pkg/parsers/registry/audittrailsv1"
	_ "github.com/transferria/transferria/pkg/parsers/registry/blank"
	_ "github.com/transferria/transferria/pkg/parsers/registry/cloudevents"
	_ "github.com/transferria/transferria/pkg/parsers/registry/cloudlogging"
	_ "github.com/transferria/transferria/pkg/parsers/registry/confluentschemaregistry"
	_ "github.com/transferria/transferria/pkg/parsers/registry/debezium"
	_ "github.com/transferria/transferria/pkg/parsers/registry/json"
	_ "github.com/transferria/transferria/pkg/parsers/registry/native"
	_ "github.com/transferria/transferria/pkg/parsers/registry/protobuf"
	_ "github.com/transferria/transferria/pkg/parsers/registry/raw2table"
	_ "github.com/transferria/transferria/pkg/parsers/registry/tskv"
)
