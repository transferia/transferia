package chrecipe

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/tests/tcrecipes"
	tc_clickhouse "github.com/transferia/transferia/tests/tcrecipes/clickhouse"
)

type ContainerParams struct {
	prefix      string
	initScripts []string
	err         error
	database    string
	user        string
}

type Option func(c *ContainerParams)

func WithPrefix(prefix string) Option {
	return func(c *ContainerParams) {
		c.prefix = prefix
	}
}

func WithDatabase(db string) Option {
	return func(c *ContainerParams) {
		c.database = db
	}
}

func WithUser(user string) Option {
	return func(c *ContainerParams) {
		c.user = user
	}
}

func WithInitFile(file string) Option {
	return func(c *ContainerParams) {
		c.initScripts = append(c.initScripts, file)
	}
}

func WithInitDir(dir string) Option {
	return func(opt *ContainerParams) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			opt.err = err
			return
		}

		for _, e := range entries {
			opt.initScripts = append(opt.initScripts, dir+"/"+e.Name())
		}
	}
}

func MustSource(opts ...Option) *model.ChSource {
	res, err := Source(opts...)
	if err != nil {
		panic(err)
	}
	return res
}

func Source(opts ...Option) (*model.ChSource, error) {
	params := ContainerParams{
		prefix:      "",
		initScripts: nil,
		err:         nil,
		database:    "mtmobproxy",
		user:        "default",
	}
	for _, opt := range opts {
		opt(&params)
	}
	if tcrecipes.Enabled() && params.err != nil {
		return nil, xerrors.Errorf("unable to prepare params: %w", params.err)
	}
	if err := Prepare(params); err != nil {
		return nil, xerrors.Errorf("unable to prepare container: %w", err)
	}
	httpPort, err := strconv.Atoi(os.Getenv(params.prefix + "RECIPE_CLICKHOUSE_HTTP_PORT"))
	if err != nil {
		return nil, xerrors.Errorf("unable to read RECIPE_CLICKHOUSE_HTTP_PORT: %w", err)
	}
	nativePort, err := strconv.Atoi(os.Getenv(params.prefix + "RECIPE_CLICKHOUSE_NATIVE_PORT"))
	if err != nil {
		return nil, xerrors.Errorf("unable to read RECIPE_CLICKHOUSE_NATIVE_PORT: %w", err)
	}
	res := &model.ChSource{
		MdbClusterID:  "",
		ChClusterName: "",
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		HTTPPort:         httpPort,
		NativePort:       nativePort,
		User:             params.user,
		Password:         "",
		SSLEnabled:       false,
		PemFileContent:   "",
		Database:         params.database,
		SubNetworkID:     "",
		SecurityGroupIDs: nil,
		IncludeTables:    nil,
		ExcludeTables:    nil,
		IsHomo:           false,
		BufferSize:       0,
		IOHomoFormat:     model.ClickhouseIOFormatCSV,
		RootCACertPaths:  nil,
		ConnectionID:     "",
	}
	res.WithDefaults()
	return res, nil
}

func MustTarget(opts ...Option) *model.ChDestination {
	res, err := Target(opts...)
	if err != nil {
		panic(err)
	}
	return res
}

func Target(opts ...Option) (*model.ChDestination, error) {
	params := ContainerParams{
		prefix:      "",
		initScripts: nil,
		err:         nil,
		database:    "mtmobproxy",
		user:        "default",
	}
	for _, opt := range opts {
		opt(&params)
	}
	if tcrecipes.Enabled() && params.err != nil {
		return nil, xerrors.Errorf("unable to prepare params: %w", params.err)
	}

	if err := Prepare(params); err != nil {
		return nil, xerrors.Errorf("unable to prepare container: %w", err)
	}

	httpPort, err := strconv.Atoi(os.Getenv(params.prefix + "RECIPE_CLICKHOUSE_HTTP_PORT"))
	if err != nil {
		return nil, xerrors.Errorf("unable to read RECIPE_CLICKHOUSE_HTTP_PORT: %w", err)
	}
	nativePort, err := strconv.Atoi(os.Getenv(params.prefix + "RECIPE_CLICKHOUSE_NATIVE_PORT"))
	if err != nil {
		return nil, xerrors.Errorf("unable to read RECIPE_CLICKHOUSE_NATIVE_PORT: %w", err)
	}
	logger.Log.Infof("clickhouse container's httpPort: %v, nativePort: %v", httpPort, nativePort)
	res := &model.ChDestination{
		MdbClusterID:            "",
		ChClusterName:           "test_shard_localhost",
		User:                    params.user,
		Password:                "",
		Database:                params.database,
		Partition:               "",
		SSLEnabled:              false,
		HTTPPort:                httpPort,
		NativePort:              nativePort,
		TTL:                     "",
		InferSchema:             false,
		MigrationOptions:        nil,
		ForceJSONMode:           false,
		ProtocolUnspecified:     true,
		AnyAsString:             false,
		SystemColumnsFirst:      false,
		IsUpdateable:            false,
		UpsertAbsentToastedRows: false,
		Hosts:                   nil,
		UseSchemaInTableName:    false,
		ShardCol:                "",
		Interval:                0,
		AltNamesList:            nil,
		ShardByTransferID:       false,
		ShardByRoundRobin:       false,
		Rotation:                nil,
		InsertParams:            model.InsertParams{MaterializedViewsIgnoreErrors: false},
		ShardsList: []model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		ColumnValueToShardNameList: nil,
		TransformerConfig:          nil,
		SubNetworkID:               "",
		SecurityGroupIDs:           nil,
		Cleanup:                    "",
		PemFileContent:             "",
		InflightBuffer:             0,
		BufferTriggingSize:         0,
		RootCACertPaths:            nil,
		ConnectionID:               "",
		IsSchemaMigrationDisabled:  false,
	}
	res.WithDefaults()
	return res, nil
}

func Prepare(params ContainerParams) error {
	if !tcrecipes.Enabled() {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// test running outside arcadia
	zk, err := tc_clickhouse.PrepareZK(ctx)
	if err != nil {
		return xerrors.Errorf("unable to prepare Zookeeper: %w", err)
	}
	fmt.Printf("zk: 0.0.0.0:%s \n", zk.Port().Port())

	chcntr, err := tc_clickhouse.Prepare(
		ctx,
		tc_clickhouse.WithDatabase("default"),
		tc_clickhouse.WithUsername(params.user),
		tc_clickhouse.WithPassword(""),
		tc_clickhouse.WithZookeeper(zk),
		tc_clickhouse.WithInitScripts(params.initScripts...),
	)
	if err != nil {
		return xerrors.Errorf("unable to start clickhouse container: %w", err)
	}
	nativePort, err := chcntr.MappedPort(ctx, tc_clickhouse.NativePort)
	if err != nil {
		return xerrors.Errorf("unable to get mapped NativePort: %w", err)
	}
	if err := os.Setenv(params.prefix+"RECIPE_CLICKHOUSE_NATIVE_PORT", nativePort.Port()); err != nil {
		return xerrors.Errorf("unable to set RECIPE_CLICKHOUSE_HTTP_PORT: %w", err)
	}
	httpPort, err := chcntr.MappedPort(ctx, tc_clickhouse.HTTPPort)
	if err != nil {
		return xerrors.Errorf("unable to get mapped HTTPPort: %w", err)
	}
	if err := os.Setenv(params.prefix+"RECIPE_CLICKHOUSE_HTTP_PORT", httpPort.Port()); err != nil {
		return xerrors.Errorf("unable to set RECIPE_CLICKHOUSE_HTTP_PORT: %w", err)
	}
	return nil
}
