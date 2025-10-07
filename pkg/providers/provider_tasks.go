package providers

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/cleanup"
	"github.com/transferia/transferia/pkg/util"
)

type TablesOperationFunc = func(table abstract.TableMap) error

type ActivateCallbacks struct {
	Cleanup       TablesOperationFunc
	Upload        TablesOperationFunc
	CheckIncludes TablesOperationFunc
	Rollbacks     *util.Rollbacks
}

var NopActivateCallback = ActivateCallbacks{
	Cleanup:       func(table abstract.TableMap) error { return nil },
	Upload:        func(table abstract.TableMap) error { return nil },
	CheckIncludes: func(table abstract.TableMap) error { return nil },
	Rollbacks:     new(util.Rollbacks),
}

// Activator enable custom functionality on transfer `Activate` task.
type Activator interface {
	Provider
	Activate(ctx context.Context, task *model.TransferOperation, table abstract.TableMap, callbacks ActivateCallbacks) error
}

// Cleanuper enable custom functionality on transfer `Activate`/`Upload`/`Reupload` tasks on `Cleanup` stage.
// If CleanupSuitable returns false then Cleanup won't be called.
type Cleanuper interface {
	Provider
	Cleanup(ctx context.Context, task *model.TransferOperation) error
	CleanupSuitable(transferType abstract.TransferType) bool
}

// Deactivator enable custom functionality on transfer `Deactivate` task.
type Deactivator interface {
	Provider
	Deactivate(ctx context.Context, task *model.TransferOperation) error
}

// Tester check that it's possible to execute provider with provided transfer params. Will return structured test result for that specific provider.
type Tester interface {
	Provider
	TestChecks() []abstract.CheckType // list of provider specific checks
	Test(ctx context.Context) *abstract.TestResult
}

// Peeker is a thing that allow to sniff data replication sample
type Sniffer interface {
	Provider
	Sniffer(ctx context.Context) (abstract.Fetchable, error)
}

// Verifier check that it's possible to execute provider with provided transfer params. Will return either OK or ERROR for specific provider.
type Verifier interface {
	Provider
	Verify(ctx context.Context) error
}

// Updater enable custom functionality on transfer `Update` tasks.
type Updater interface {
	Provider
	Update(ctx context.Context, addedTables []abstract.TableDescription) error
}

type Cleaner interface {
	cleanup.Closeable
	CleanupTmp(ctx context.Context, transferID string, tmpPolicy *model.TmpPolicyConfig) error
}

// TMPCleaner enable custom functionality on transfer `TMP Policy` inside `Cleanup` stage of `Activate` task.
type TMPCleaner interface {
	Provider
	TMPCleaner(ctx context.Context, task *model.TransferOperation) (Cleaner, error)
}
