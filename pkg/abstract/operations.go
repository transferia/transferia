package abstract

import (
	"reflect"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/set"
)

type Task interface {
	Visit(visitor TaskVisitor) interface{}
	isTaskType()
}

type RunnableTask interface {
	Task
	VisitRunnable(visitor RunnableVisitor) interface{}
	isRunnable()
}

type ShardableTask interface {
	Task
	isShardableTask()
}

type EphemeralTask interface {
	Task
	isEphemeralTask()
}

type FakeTask interface {
	Task
	VisitFake(visitor FakeVisitor) interface{}
	isFake()
}

type TimeoutableTask interface {
	Task
	HasTimedOut(createdAt time.Time, pingedAt time.Time) bool
}

// Any task is either runnable or fake; embed one of these two types in a new task
type (
	runnable struct{}
	fake     struct{}
)

// Runnable tasks {{{
type (
	Activate struct {
		runnable
	}
	Upload struct {
		runnable
	}
	ReUpload struct {
		runnable
	}
	Start struct {
		runnable
	}
	Restart struct {
		runnable
	}
	Stop struct {
		runnable
	}
	Verify struct {
		runnable
	}
	AddTables struct {
		runnable
	}
	UpdateTransfer struct {
		runnable
	}
	RemoveTables struct {
		runnable
	}
	Deactivate struct {
		runnable
	}
	CleanupResource struct {
		runnable
	}
	Checksum struct {
		runnable
	}
	TestEndpoint struct {
		runnable
	}
)

func (Activate) isShardableTask()       {}
func (ReUpload) isShardableTask()       {}
func (Upload) isShardableTask()         {}
func (AddTables) isShardableTask()      {}
func (UpdateTransfer) isShardableTask() {}

func (Termination) isEphemeralTask() {}
func (Stop) isEphemeralTask()        {}
func (Start) isEphemeralTask()       {}

var RunnableTasks []RunnableTask = makeRunnableTasks()

// }}}

// Fake tasks {{{
type (
	Replication             struct{ fake }
	Termination             struct{ fake }
	EndpointDelete          struct{ fake }
	TransferCreate          struct{ fake }
	TransferDelete          struct{ fake }
	TransferVersionUpdate   struct{ fake }
	TransferVersionFreeze   struct{ fake }
	TransferVersionUnfreeze struct{ fake }
)

var FakeTasks []FakeTask = makeFakeTasks()

// }}}

// Parameters {{{

type TransferVersionUpdateParams struct {
	PreviousVersion string `json:"previous_version"`
	CurrentVersion  string `json:"current_version"`
}

type TransferVersionFreezeParams struct {
	CurrentVersion string `json:"current_version"`
}

type TransferVersionUnfreezeParams struct {
	CurrentVersion string `json:"current_version"`
}

type UpdateTransferParams struct {
	OldObjects []string `json:"old_objects"`
	NewObjects []string `json:"new_objects"`
}

func (p UpdateTransferParams) AddedTables() ([]TableDescription, error) {
	olds, err := ParseTableIDs(p.OldObjects...)
	if err != nil {
		return nil, xerrors.Errorf("invalid old objects: %w", err)
	}
	news, err := ParseTableIDs(p.NewObjects...)
	if err != nil {
		return nil, xerrors.Errorf("invalid new objects: %w", err)
	}
	oldSet := set.New(olds...)
	var tables []TableDescription
	for _, obj := range news {
		if oldSet.Contains(obj) {
			continue
		}
		tables = append(tables, TableDescription{
			Name:   obj.Name,
			Schema: obj.Namespace,
			Filter: "",
			EtaRow: 0,
			Offset: 0,
		})
	}
	return tables, nil
}

// }}}

var AllTasks []Task = makeAllTasks()

// Visitors {{{
type TaskVisitor interface {
	RunnableVisitor
	FakeVisitor
}

type RunnableVisitor interface {
	OnActivate(t Activate) interface{}
	OnAddTables(t AddTables) interface{}
	OnUpdateTransfer(t UpdateTransfer) interface{}
	OnChecksum(t Checksum) interface{}
	OnDeactivate(t Deactivate) interface{}
	OnCleanupResource(t CleanupResource) interface{}
	OnReUpload(t ReUpload) interface{}
	OnRemoveTables(t RemoveTables) interface{}
	OnRestart(t Restart) interface{}
	OnStart(t Start) interface{}
	OnStop(t Stop) interface{}
	OnUpload(t Upload) interface{}
	OnVerify(t Verify) interface{}
	OnTestEndpoint(t TestEndpoint) interface{}
}

type FakeVisitor interface {
	OnEndpointDelete(t EndpointDelete) interface{}
	OnTransferCreate(t TransferCreate) interface{}
	OnTransferDelete(t TransferDelete) interface{}
	OnReplication(t Replication) interface{}
	OnTermination(t Termination) interface{}
	OnTransferVersionUpdate(t TransferVersionUpdate) interface{}
	OnTransferVersionFreeze(t TransferVersionFreeze) interface{}
	OnTransferVersionUnfreeze(t TransferVersionUnfreeze) interface{}
}

// }}}

// Method implementations {{{
func (t runnable) isTaskType() {}
func (t runnable) isRunnable() {}
func (t fake) isTaskType()     {}
func (t fake) isFake()         {}

func (t Activate) VisitRunnable(v RunnableVisitor) interface{}        { return v.OnActivate(t) }
func (t Upload) VisitRunnable(v RunnableVisitor) interface{}          { return v.OnUpload(t) }
func (t ReUpload) VisitRunnable(v RunnableVisitor) interface{}        { return v.OnReUpload(t) }
func (t Start) VisitRunnable(v RunnableVisitor) interface{}           { return v.OnStart(t) }
func (t Restart) VisitRunnable(v RunnableVisitor) interface{}         { return v.OnRestart(t) }
func (t Stop) VisitRunnable(v RunnableVisitor) interface{}            { return v.OnStop(t) }
func (t Verify) VisitRunnable(v RunnableVisitor) interface{}          { return v.OnVerify(t) }
func (t AddTables) VisitRunnable(v RunnableVisitor) interface{}       { return v.OnAddTables(t) }
func (t RemoveTables) VisitRunnable(v RunnableVisitor) interface{}    { return v.OnRemoveTables(t) }
func (t Deactivate) VisitRunnable(v RunnableVisitor) interface{}      { return v.OnDeactivate(t) }
func (t CleanupResource) VisitRunnable(v RunnableVisitor) interface{} { return v.OnCleanupResource(t) }
func (t Checksum) VisitRunnable(v RunnableVisitor) interface{}        { return v.OnChecksum(t) }
func (t TestEndpoint) VisitRunnable(v RunnableVisitor) interface{}    { return v.OnTestEndpoint(t) }
func (t UpdateTransfer) VisitRunnable(v RunnableVisitor) interface{} {
	return v.OnUpdateTransfer(t)
}

func (t Activate) Visit(v TaskVisitor) interface{}        { return t.VisitRunnable(v) }
func (t Upload) Visit(v TaskVisitor) interface{}          { return t.VisitRunnable(v) }
func (t ReUpload) Visit(v TaskVisitor) interface{}        { return t.VisitRunnable(v) }
func (t Start) Visit(v TaskVisitor) interface{}           { return t.VisitRunnable(v) }
func (t Restart) Visit(v TaskVisitor) interface{}         { return t.VisitRunnable(v) }
func (t Stop) Visit(v TaskVisitor) interface{}            { return t.VisitRunnable(v) }
func (t Verify) Visit(v TaskVisitor) interface{}          { return t.VisitRunnable(v) }
func (t AddTables) Visit(v TaskVisitor) interface{}       { return t.VisitRunnable(v) }
func (t RemoveTables) Visit(v TaskVisitor) interface{}    { return t.VisitRunnable(v) }
func (t Deactivate) Visit(v TaskVisitor) interface{}      { return t.VisitRunnable(v) }
func (t CleanupResource) Visit(v TaskVisitor) interface{} { return t.VisitRunnable(v) }
func (t Checksum) Visit(v TaskVisitor) interface{}        { return t.VisitRunnable(v) }
func (t TestEndpoint) Visit(v TaskVisitor) interface{}    { return t.VisitRunnable(v) }
func (t UpdateTransfer) Visit(v TaskVisitor) interface{}  { return t.VisitRunnable(v) }

func (t Replication) VisitFake(v FakeVisitor) interface{}    { return v.OnReplication(t) }
func (t Termination) VisitFake(v FakeVisitor) interface{}    { return v.OnTermination(t) }
func (t EndpointDelete) VisitFake(v FakeVisitor) interface{} { return v.OnEndpointDelete(t) }
func (t TransferCreate) VisitFake(v FakeVisitor) interface{} { return v.OnTransferCreate(t) }
func (t TransferDelete) VisitFake(v FakeVisitor) interface{} { return v.OnTransferDelete(t) }
func (t TransferVersionUpdate) VisitFake(v FakeVisitor) interface{} {
	return v.OnTransferVersionUpdate(t)
}
func (t TransferVersionFreeze) VisitFake(v FakeVisitor) interface{} {
	return v.OnTransferVersionFreeze(t)
}
func (t TransferVersionUnfreeze) VisitFake(v FakeVisitor) interface{} {
	return v.OnTransferVersionUnfreeze(t)
}

func (t Replication) Visit(v TaskVisitor) interface{}    { return t.VisitFake(v) }
func (t Termination) Visit(v TaskVisitor) interface{}    { return t.VisitFake(v) }
func (t EndpointDelete) Visit(v TaskVisitor) interface{} { return t.VisitFake(v) }
func (t TransferCreate) Visit(v TaskVisitor) interface{} { return t.VisitFake(v) }
func (t TransferDelete) Visit(v TaskVisitor) interface{} { return t.VisitFake(v) }
func (t TransferVersionUpdate) Visit(v TaskVisitor) interface{} {
	return v.OnTransferVersionUpdate(t)
}
func (t TransferVersionFreeze) Visit(v TaskVisitor) interface{} {
	return v.OnTransferVersionFreeze(t)
}
func (t TransferVersionUnfreeze) Visit(v TaskVisitor) interface{} {
	return v.OnTransferVersionUnfreeze(t)
}

func (t TestEndpoint) HasTimedOut(createdAt time.Time, pingedAt time.Time) bool {
	if pingedAt.IsZero() {
		return time.Since(createdAt) > time.Minute*5
	}
	return time.Since(pingedAt) > time.Minute*5
}

func (t Verify) HasTimedOut(createdAt time.Time, pingedAt time.Time) bool {
	if pingedAt.IsZero() {
		return time.Since(createdAt) > time.Minute*5
	}
	return time.Since(pingedAt) > time.Minute*5
}

// }}}

// Iteration {{{

func makeRunnableTasks() (result []RunnableTask) {
	visitorType := reflect.TypeOf((*RunnableVisitor)(nil)).Elem()
	for i := 0; i < visitorType.NumMethod(); i++ {
		method := visitorType.Method(i)
		argType := method.Type.In(0)
		value := reflect.Zero(argType)
		result = append(result, value.Interface().(RunnableTask))
	}
	return result
}

func makeFakeTasks() (result []FakeTask) {
	visitorType := reflect.TypeOf((*FakeVisitor)(nil)).Elem()
	for i := 0; i < visitorType.NumMethod(); i++ {
		method := visitorType.Method(i)
		argType := method.Type.In(0)
		value := reflect.Zero(argType)
		result = append(result, value.Interface().(FakeTask))
	}
	return result
}

func makeAllTasks() (result []Task) {
	for _, t := range RunnableTasks {
		result = append(result, t)
	}
	for _, t := range FakeTasks {
		result = append(result, t)
	}
	return result
}

// }}}
