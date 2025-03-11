package registry

import (
	_ "github.com/transferia/transferia/pkg/transformer/registry/batch_splitter"
	_ "github.com/transferia/transferia/pkg/transformer/registry/clickhouse"
	_ "github.com/transferia/transferia/pkg/transformer/registry/custom"
	_ "github.com/transferia/transferia/pkg/transformer/registry/filter"
	_ "github.com/transferia/transferia/pkg/transformer/registry/filter_rows"
	_ "github.com/transferia/transferia/pkg/transformer/registry/logger"
	_ "github.com/transferia/transferia/pkg/transformer/registry/mask"
	_ "github.com/transferia/transferia/pkg/transformer/registry/number_to_float"
	_ "github.com/transferia/transferia/pkg/transformer/registry/problem_item_detector"
	_ "github.com/transferia/transferia/pkg/transformer/registry/raw_doc_grouper"
	_ "github.com/transferia/transferia/pkg/transformer/registry/rename"
	_ "github.com/transferia/transferia/pkg/transformer/registry/replace_primary_key"
	_ "github.com/transferia/transferia/pkg/transformer/registry/sharder"
	_ "github.com/transferia/transferia/pkg/transformer/registry/table_splitter"
	_ "github.com/transferia/transferia/pkg/transformer/registry/to_string"
	_ "github.com/transferia/transferia/pkg/transformer/registry/yt_dict"
)
