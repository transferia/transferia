package model

import (
	"encoding/json"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	transformers_registry "github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/util"
)

type Transfer struct {
	ID                string
	TransferName      string
	Description       string
	Labels            string
	Status            TransferStatus
	Type              abstract.TransferType
	Runtime           abstract.Runtime
	Src               Source
	Dst               Destination
	RegularSnapshot   *abstract.RegularSnapshot
	Transformation    *Transformation
	DataObjects       *DataObjects
	TypeSystemVersion int
	TmpPolicy         *TmpPolicyConfig

	AsyncOperations bool // real async operation flag

	// TODO: remove
	FolderID string
	CloudID  string
	Author   string
}

const (
	// LatestVersion is the current (most recent) version of the typesystem. Increment this when adding a new fallback.
	//
	// At any moment, fallbacks can only be "to" any version preceding the latest.
	//
	// Zero value is reserved and MUST NOT be used.
	//
	// When incrementing this value, DO ADD a link to the function(s) implementing this fallback to CHANGELOG.md in the current directory
	LatestVersion int = 10
	// NewTransfersVersion is the version of the typesystem set for new transfers. It must be less or equal to the LatestVersion.
	//
	// To upgrade typesystem version, the following process should be applied:
	// 1. LatestVersion is increased & fallbacks are introduced in the first PR. NewTransfersVersion stays the same!
	// 2. Controlplane and dataplane are deployed and dataplane now contains the fallbacks for a new version.
	// 3. The second PR increases NewTransfersVersion. When a controlplane with this change is deployed, dataplanes already have the required fallbacks.
	NewTransfersVersion int = 10
)

func (f *Transfer) SnapshotOnly() bool {
	return f.Type == abstract.TransferTypeSnapshotOnly
}

func (f *Transfer) IncrementOnly() bool {
	return f.Type == abstract.TransferTypeIncrementOnly
}

func (f *Transfer) RuntimeType() string {
	if f.Runtime != nil {
		return string(f.Runtime.Type())
	}
	return "local"
}

func (f *Transfer) DstType() abstract.ProviderType {
	if f == nil || f.Dst == nil {
		return abstract.ProviderTypeNone
	}
	return f.Dst.GetProviderType()
}

func (f *Transfer) DstJSON() string {
	b, err := json.Marshal(f.Dst)
	if err != nil {
		return ""
	}
	return string(b)
}

func (f *Transfer) SrcType() abstract.ProviderType {
	if f == nil || f.Src == nil {
		return abstract.ProviderTypeNone
	}
	return f.Src.GetProviderType()
}

func (f *Transfer) SrcJSON() string {
	b, err := json.Marshal(f.Src)
	if err != nil {
		return ""
	}
	return string(b)
}

func (f *Transfer) WithDefault() {
	if f.Src != nil {
		f.Src.WithDefaults()
	}
	if f.Dst != nil {
		f.Dst.WithDefaults()
	}
	if f.Runtime != nil {
		f.Runtime.WithDefaults()
	}
}

// FillDependentFields
//
// Should be called every time when new transfer object created
// For now, it's called:
// - in repository, when building transfer object from the db
// - in transitive uploading, when creating synthetic transfer
// - in e2e-tests, when building transfer object from endpoints (helpers: MakeTransfer/InitSrcDst)
func (f *Transfer) FillDependentFields() {
	if f.TypeSystemVersion == 0 {
		// the LatestVersion is used here instead of the NewTransfersVersion for test purposes
		f.TypeSystemVersion = LatestVersion
	}
	if schmegacy, ok := f.Src.(LegacyFillDependentFields); ok {
		schmegacy.FillDependentFields(f)
	}
	if schmegacy, ok := f.Dst.(LegacyFillDependentFields); ok {
		schmegacy.FillDependentFields(f)
	}
}

func (f *Transfer) IsAbstract2() bool {
	switch e := f.Src.(type) {
	case Abstract2Source:
		return e.IsAbstract2(f.Dst)
	default:
		return false
	}
}

func (f *Transfer) Validate() error {
	if !f.SnapshotOnly() && f.RegularSnapshot != nil && f.RegularSnapshot.Enabled {
		return xerrors.Errorf("regular snapshot not supported for %v transfer", f.Type)
	}
	if f.RegularSnapshot != nil && len(f.RegularSnapshot.Incremental) > 0 {
		if includes, ok := f.Src.(Includeable); ok {
			var missedTables []abstract.TableID
			for _, table := range f.RegularSnapshot.Incremental {
				if !includes.Include(table.TableID()) {
					missedTables = append(missedTables, table.TableID())
				}
			}
			if len(missedTables) > 0 {
				return xerrors.Errorf("invalid incremental tables: missed %v tables in source: %v", len(missedTables), missedTables)
			}
		}
	}
	if dst, ok := f.Dst.(SourceCompatibility); ok {
		if err := dst.Compatible(f.Src, f.Type); err != nil {
			return xerrors.Errorf("target is not compatible with source: %w", err)
		}
	}
	if src, ok := f.Src.(DestinationCompatibility); ok {
		if err := src.Compatible(f.Dst); err != nil {
			return xerrors.Errorf("source is not compatible with target: %w", err)
		}
	}

	return nil
}

func (f *Transfer) ValidateDataObjects() error {
	tableSet := map[abstract.TableID]bool{}
	for _, obj := range f.DataObjects.IncludeObjects {
		tid, err := abstract.ParseTableID(obj)
		if err != nil {
			return xerrors.Errorf("unable to parse obj: %s: %w", obj, err)
		}
		if !f.Include(*tid) {
			return xerrors.Errorf("table: %s not included in source", obj)
		}
		if tableSet[*tid] {
			return xerrors.Errorf("dublicate table: %s", obj)
		}
		tableSet[*tid] = true
	}
	return nil
}

func (f *Transfer) RegularSnapshotJSON() []byte {
	data, err := json.Marshal(f.RegularSnapshot)
	if err != nil {
		return []byte(`{}`)
	}
	return data
}

func (f *Transfer) RegularSnapshotEnabled() bool {
	return f.RegularSnapshot != nil && f.RegularSnapshot.Enabled
}

func (f *Transfer) HasTransformation() bool {
	return f.HasPublicTransformation() || f.HasExtraTransformation()
}

func (f *Transfer) TransformationConfigs() []transformers_registry.Transformer {
	if !f.HasPublicTransformation() {
		return nil
	}
	return f.Transformation.Transformers.Transformers
}

func (f *Transfer) HasPublicTransformation() bool {
	return f.Transformation != nil &&
		f.Transformation.Transformers != nil &&
		len(f.Transformation.Transformers.Transformers) > 0
}

func (f *Transfer) HasExtraTransformation() bool {
	return f.Transformation != nil && len(f.Transformation.ExtraTransformers) > 0
}

func (f *Transfer) AddExtraTransformer(transformer abstract.Transformer) error {
	if f.Transformation == nil {
		f.Transformation = new(Transformation)
	}
	f.Transformation.ExtraTransformers = append(f.Transformation.ExtraTransformers, transformer)
	if f.Transformation.Executor != nil {
		// add new transformer to transformation executor plan
		return f.Transformation.Executor.AddTransformer(transformer)
	}
	return nil
}

func (f *Transfer) TransformationMiddleware() (abstract.SinkOption, error) {
	if f.Transformation != nil {
		if f.Transformation.Executor == nil {
			return nil, xerrors.New("Transformation executor is not inited")
		}
		return f.Transformation.Executor.MakeSinkMiddleware(), nil
	}
	return nil, nil
}

func (f *Transfer) TransformationJSON() ([]byte, error) {
	if f.Transformation == nil || f.Transformation.Transformers == nil {
		return []byte(`{}`), nil
	}

	return json.Marshal(f.Transformation.Transformers)
}

func (f *Transfer) TransformationFromJSON(value string) error {
	transformation, err := MakeTransformationFromJSON(value)
	if err != nil {
		return xerrors.Errorf("cannot make transformation from JSON string: %w", err)
	}
	f.Transformation = transformation
	return nil
}

// IsTransitional show transfer that used by kostya and burn our pukans
func (f *Transfer) IsTransitional() bool {
	if _, ok := f.Src.(TransitionalEndpoint); ok {
		return true
	}
	if _, ok := f.Dst.(TransitionalEndpoint); ok {
		return true
	}
	return false
}

func (f *Transfer) IsIncremental() bool {
	if !f.SnapshotOnly() {
		return false
	}
	if f.RegularSnapshot == nil || len(f.RegularSnapshot.Incremental) == 0 {
		return false
	}
	return true
}

func (f *Transfer) CanReloadFromState() bool {
	if f.IncrementOnly() {
		return false
	}
	if f.RegularSnapshot == nil || len(f.RegularSnapshot.Incremental) == 0 {
		return false
	}
	return true
}

func (f *Transfer) ParallelismParams() *abstract.ShardUploadParams {
	parallelismParams := abstract.DefaultShardUploadParams()

	if paralleledRuntime, ok := f.Runtime.(abstract.ShardingTaskRuntime); ok {
		if paralleledRuntime.WorkersNum() > 0 {
			parallelismParams.JobCount = paralleledRuntime.WorkersNum()
		}
		if paralleledRuntime.ThreadsNumPerWorker() > 0 {
			parallelismParams.ProcessCount = paralleledRuntime.ThreadsNumPerWorker()
		}
	}

	if serializer, ok := f.Dst.(Serializable); ok {
		_, saveTxOrder := serializer.Serializer()
		if saveTxOrder {
			parallelismParams.ProcessCount = 1
		}
	}

	return parallelismParams
}

func (f *Transfer) IsSharded() bool {
	if rt, ok := f.Runtime.(abstract.ShardingTaskRuntime); ok {
		return rt.WorkersNum() > 1
	}
	return false
}

func (f *Transfer) IsMain() bool {
	if rt, ok := f.Runtime.(abstract.ShardingTaskRuntime); ok {
		return rt.IsMain()
	}
	return true
}

func (f *Transfer) CurrentJobIndex() int {
	if f.Runtime == nil {
		return 0
	}
	if rt, ok := f.Runtime.(abstract.ShardingTaskRuntime); ok {
		return rt.CurrentJobIndex()
	}
	return 0
}

func (f *Transfer) DataObjectsFromJSON(objects string) error {
	if objects == "{}" || objects == "null" || objects == "" {
		return nil
	}
	objs := new(DataObjects)
	err := json.Unmarshal([]byte(objects), objs)
	if err != nil {
		return xerrors.Errorf("unable to load data objects json: %w", err)
	}
	f.DataObjects = objs
	return nil
}

func (f *Transfer) DataObjectsJSON() (string, error) {
	j, err := json.Marshal(f.DataObjects)
	return string(j), err
}

func (f *Transfer) FilterObjects(result abstract.TableMap) (abstract.TableMap, error) {
	if f.DataObjects == nil || len(f.DataObjects.IncludeObjects) == 0 {
		return result, nil
	}
	var errs util.Errors
	res := map[abstract.TableID]abstract.TableInfo{}
	for _, obj := range f.DataObjects.IncludeObjects {
		tid, err := abstract.ParseTableID(obj)
		if err != nil {
			errs = append(errs, xerrors.Errorf("unable to parse obj: %s: %w", obj, err))
			continue
		}
		info, ok := result[*tid]
		if !ok {
			errs = append(errs, xerrors.Errorf("object: %s not found in source", obj))
			continue
		}
		res[*tid] = info
	}
	if len(errs) > 0 {
		return nil, xerrors.Errorf("unable to filter transfer objects: %w", errs)
	}
	return res, nil
}

func (f *Transfer) IncludeTableList() ([]abstract.TableID, error) {
	var res []abstract.TableID
	if f.DataObjects != nil {
		for _, obj := range f.DataObjects.IncludeObjects {
			tid, err := abstract.ParseTableID(obj)
			if err != nil {
				return nil, xerrors.Errorf("unable to parse object: %s: %w", obj, err)
			}
			res = append(res, *tid)
		}
	}
	return res, nil
}

func (f *Transfer) Include(tID abstract.TableID) bool {
	if f.DataObjects == nil || len(f.DataObjects.IncludeObjects) == 0 {
		return true
	}
	for _, obj := range f.DataObjects.IncludeObjects {
		parsedTID, _ := abstract.ParseTableID(obj)
		if parsedTID != nil && *parsedTID == tID {
			return true
		}
	}
	return false
}

// SystemLabel method is used to access system labels for transfer.
// System labels are special reserved labels which are used to control some
// hidden experimental transfer features
func (f *Transfer) SystemLabel(name SystemLabel) (string, error) {
	labelMap := map[string]string{}
	if err := json.Unmarshal([]byte(f.Labels), &labelMap); err != nil {
		return "", xerrors.Errorf("error parsing transfer labels: %w", err)
	}
	val, ok := labelMap[string(name)]
	if !ok {
		return "", xerrors.Errorf("transfer label '%s' is not set", name)
	}
	return val, nil
}

func (f *Transfer) LabelsRaw() string {
	return f.Labels
}

func (f *Transfer) Copy(name string) Transfer {
	return Transfer{
		ID:                name,
		TransferName:      f.TransferName,
		Description:       f.Description,
		Labels:            f.Labels,
		Status:            f.Status,
		Type:              f.Type,
		Runtime:           f.Runtime,
		Src:               f.Src,
		Dst:               f.Dst,
		RegularSnapshot:   f.RegularSnapshot,
		Transformation:    f.Transformation,
		DataObjects:       f.DataObjects,
		TypeSystemVersion: f.TypeSystemVersion,
		TmpPolicy:         f.TmpPolicy,
		FolderID:          f.FolderID,
		CloudID:           f.CloudID,
		Author:            f.Author,
		AsyncOperations:   f.AsyncOperations,
	}
}

func (f *Transfer) IsAsyncCHExp() bool {
	val, err := f.SystemLabel(SystemLabelAsyncCH)
	if err != nil || val != "on" {
		return false
	}
	_, isTrueAsyncSource := f.Src.(AsyncPartSource)
	return isTrueAsyncSource
}
