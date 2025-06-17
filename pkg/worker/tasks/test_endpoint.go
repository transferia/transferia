package tasks

import (
	"context"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/middlewares"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/storage"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/util"
)

type EndpointParam struct {
	Type  abstract.ProviderType
	Param string
}

type TestEndpointParams struct {
	Transfer             *model.Transfer
	TransformationConfig []byte
	ParamsSrc            *EndpointParam
	ParamsDst            *EndpointParam
}

const (
	CredentialsCheckType   = abstract.CheckType("credentials")
	DataSampleCheckType    = abstract.CheckType("data-sample")
	ConfigCheckType        = abstract.CheckType("config-valid-check")
	WriteableCheckType     = abstract.CheckType("writeable")
	LoadTablesCheckType    = abstract.CheckType("load-all-tables")
	EstimateTableCheckType = abstract.CheckType("estimate-table")
	LoadSampleCheckType    = abstract.CheckType("load-sample")
)

func SniffReplicationData(
	ctx context.Context,
	sniffer abstract.Fetchable,
	tr *abstract.TestResult,
	transfer *model.Transfer,
) *abstract.TestResult {
	res, err := sniffer.Fetch()
	if err != nil {
		return tr.NotOk(DataSampleCheckType, errors.CategorizedErrorf(categories.Source, "unable to read sample: %w", err))
	}
	previewMap := map[abstract.TableID][]abstract.ChangeItem{}
	for _, row := range res {
		previewMap[row.TableID()] = append(previewMap[row.TableID()], row)
	}
	if transfer.HasPublicTransformation() {
		for table, rows := range previewMap {
			collector := &sampleCollector{
				res:    map[abstract.TableID][]abstract.ChangeItem{},
				ctx:    context.Background(),
				cancel: func() {},
			}
			transfer.Transformation.Transformers.ErrorsOutput = &transformer.ErrorsOutput{
				// force sink error, so user can see all transformer errors in discover results
				Type:   transformer.SinkErrorsOutput,
				Config: nil,
			}
			wrapper, err := middlewares.Transformation(transfer, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
			if err != nil {
				return tr.NotOk(ConfigCheckType, xerrors.Errorf("unable to assign transformer: %w", err))
			}
			if err := wrapper(collector).Push(rows); err != nil {
				return tr.NotOk(DataSampleCheckType, xerrors.Errorf("unable to push items to transformation: %w", err))
			}
			previewMap[table] = collector.res[table]
		}
	}

	tr.Ok(ConfigCheckType)
	tr.Ok(DataSampleCheckType)
	tr.Ok(ConfigCheckType)

	tr.Preview = previewMap
	return tr
}

var _ abstract.Sinker = (*sampleCollector)(nil)

type sampleCollector struct {
	res    map[abstract.TableID][]abstract.ChangeItem
	ctx    context.Context
	cancel func()
}

func (s *sampleCollector) Close() error {
	return nil
}

func (s *sampleCollector) Push(items []abstract.ChangeItem) error {
	for _, item := range items {
		if len(s.res[item.TableID()]) > 3 {
			s.cancel()
			continue
		}
		if item.IsRowEvent() {
			s.res[item.TableID()] = append(s.res[item.TableID()], item)
		}
	}
	return nil
}

func SniffSnapshotData(ctx context.Context, tr *abstract.TestResult, transfer *model.Transfer) *abstract.TestResult {
	metrics := solomon.NewRegistry(solomon.NewRegistryOpts())
	tables, err := ObtainAllSrcTables(transfer, metrics)
	if err != nil {
		return tr.NotOk(LoadTablesCheckType, errors.CategorizedErrorf(categories.Source, "unable to load table list: %w", err))
	}
	tr.Ok(LoadTablesCheckType)

	sourceStorage, err := storage.NewStorage(transfer, coordinator.NewFakeClient(), metrics)
	if err != nil {
		return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Source, ResolveStorageErrorText, err))
	}
	tr.Ok(ConfigCheckType)
	tr.Preview = map[abstract.TableID][]abstract.ChangeItem{}
	defer sourceStorage.Close()
	var errs util.Errors
	previewSchemas := abstract.TableMap{}
	for table := range tables {
		cctx, cancel := context.WithTimeout(ctx, time.Minute)
		collector := &sampleCollector{
			res:    map[abstract.TableID][]abstract.ChangeItem{},
			ctx:    cctx,
			cancel: cancel,
		}
		wrapper, err := middlewares.Transformation(transfer, logger.Log, metrics)
		if err != nil {
			return tr.NotOk(ConfigCheckType, xerrors.Errorf("unable to assign transformer: %w", err))
		}
		sinker := wrapper(collector)
		if abstract.IsSystemTable(table.Name) {
			continue
		}
		tdesc := abstract.TableDescription{
			Name:   table.Name,
			Schema: table.Namespace,
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		}

		cnt, err := sourceStorage.EstimateTableRowsCount(table)
		if err != nil {
			exactCnt, exactErr := sourceStorage.ExactTableRowsCount(table)
			if exactErr != nil {
				return tr.NotOk(EstimateTableCheckType, xerrors.Errorf("unable to extract table rows count: estimate count: %w\nexact count: %w", err, exactErr))
			}
			cnt = exactCnt
		}

		if sampleable, ok := sourceStorage.(abstract.SampleableStorage); ok && cnt > 2000 {
			err = sampleable.LoadRandomSample(tdesc, sinker.Push)
		} else {
			err = sourceStorage.LoadTable(cctx, tdesc, sinker.Push)
		}

		if err != nil && len(collector.res) == 0 {
			logger.Log.Warnf("unable to load: %v: %v", table, err)
			categorizedErr := errors.ToTransferStatusMessage(err)
			errs = append(errs, xerrors.Errorf("failed to load: %s: %s: %s", tdesc.String(), categorizedErr.Categories, categorizedErr.Heading))
		}
		for tid, items := range collector.res {
			if len(items) == 0 {
				schema, err := sourceStorage.TableSchema(ctx, tid)
				if err != nil {
					return tr.NotOk(LoadSampleCheckType, xerrors.Errorf("unable to load: %s schema: %w", tid.Fqtn(), err))
				}
				previewSchemas[tid] = abstract.TableInfo{
					EtaRow: 0,
					IsView: false,
					Schema: schema,
				}
				continue
			}
			previewSchemas[tid] = abstract.TableInfo{
				EtaRow: 0,
				IsView: false,
				Schema: items[0].TableSchema,
			}
			if len(items) > 3 {
				items = items[:3]
			}
			tr.Preview[tid] = items
		}
	}
	tr.Schema = previewSchemas

	if len(errs) > 0 {
		return tr.NotOk(LoadSampleCheckType, xerrors.Errorf("found failures %v: %w", len(errs), errs))
	}

	tr.Ok(EstimateTableCheckType)
	tr.Ok(LoadSampleCheckType)
	return tr
}

func TestEndpoint(ctx context.Context, param *TestEndpointParams, tr *abstract.TestResult) *abstract.TestResult {
	var err error
	if param.ParamsDst != nil {
		tr, err = PrepareTargetChecks(param, tr)
		if err != nil {
			return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Internal, "unable to prepare target checks: %w", err))
		}
		tr = TestDestinationEndpoint(ctx, param, tr)
	}
	if param.ParamsSrc != nil {
		tr, err = PrepareSourceChecks(param, tr)
		if err != nil {
			return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Internal, "unable to prepare source checks: %w", err))
		}
		tr = TestSourceEndpoint(ctx, param, tr)
	}
	return tr
}

func TestDestinationEndpoint(ctx context.Context, param *TestEndpointParams, tr *abstract.TestResult) *abstract.TestResult {
	dst, err := model.NewDestination(param.ParamsDst.Type, param.ParamsDst.Param)
	if err != nil {
		return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Target, "unable to construct target: %w", err))
	}
	tr.Ok(ConfigCheckType)
	param.Transfer.Dst = dst
	param.Transfer.Src = new(model.MockSource)
	if err := TestTargetEndpoint(param.Transfer); err != nil {
		return tr.NotOk(WriteableCheckType, errors.CategorizedErrorf(categories.Target, "unable to test target endpoint: %w", err))
	}
	tr.Ok(WriteableCheckType)
	return tr
}

func TestSourceEndpoint(ctx context.Context, param *TestEndpointParams, tr *abstract.TestResult) *abstract.TestResult {
	param.Transfer.Dst = new(model.MockDestination)
	endpointSource, err := model.NewSource(param.ParamsSrc.Type, param.ParamsSrc.Param)
	if err != nil {
		return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Source, "unable to get endpoint endpointSource: %w", err))
	}
	param.Transfer.Src = endpointSource
	if len(param.TransformationConfig) > 0 {
		if err := param.Transfer.TransformationFromJSON(string(param.TransformationConfig)); err != nil {
			return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Internal, "unable to restore transform config: %w", err))
		}
	}
	tr.Ok(ConfigCheckType)

	if tester, ok := providers.Source[providers.Tester](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		partialTr := tester.Test(ctx)
		tr.Combine(partialTr)
		if tr.Err() != nil {
			return tr
		}
	}
	if _, ok := providers.Source[providers.Snapshot](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		return SniffSnapshotData(ctx, tr, param.Transfer)
	}

	if snifferP, ok := providers.Source[providers.Sniffer](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		sniffer, err := snifferP.Sniffer(ctx)
		if err != nil {
			return tr.NotOk(ConfigCheckType, errors.CategorizedErrorf(categories.Source, "unable to construct sniffer: %w", err))
		}
		tr.Ok(CredentialsCheckType)
		return SniffReplicationData(ctx, sniffer, tr, param.Transfer)
	}
	return tr
}

// PrepareSourceChecks registers all source specific check.
// These checks also contain the providers specific checks returned by the ToTest method.
func PrepareSourceChecks(param *TestEndpointParams, tr *abstract.TestResult) (*abstract.TestResult, error) {
	tr.Add(ConfigCheckType)

	param.Transfer.Dst = new(model.MockDestination)

	endpointSource, err := model.NewSource(param.ParamsSrc.Type, param.ParamsSrc.Param)
	if err != nil {
		return nil, xerrors.Errorf("unable to get endpoint endpointSource: %w", err)
	}
	param.Transfer.Src = endpointSource

	if tester, ok := providers.Source[providers.Tester](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		tr.Add(tester.TestChecks()...)
	}
	if _, ok := providers.Source[providers.Snapshot](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		tr.Add(LoadTablesCheckType, EstimateTableCheckType, LoadSampleCheckType)
		return tr, nil
	}

	if _, ok := providers.Source[providers.Sniffer](
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
		coordinator.NewFakeClient(),
		param.Transfer,
	); ok {
		tr.Add(CredentialsCheckType, DataSampleCheckType, ConfigCheckType)
	}

	return tr, nil
}

// PrepareTargetChecks registers all target specific check.
func PrepareTargetChecks(param *TestEndpointParams, tr *abstract.TestResult) (*abstract.TestResult, error) {
	tr.Add(ConfigCheckType, WriteableCheckType)
	return tr, nil
}
