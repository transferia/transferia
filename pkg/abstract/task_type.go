package abstract

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/gobwrapper"
)

type TaskTypeName = string

var TaskTypeByName map[TaskTypeName]TaskType = makeTaskTypeByNameMap()

//nolint:exhaustivestruct
var SupportedAsyncOperations = map[string]bool{
	TaskType{Task: Activate{}}.String():              true,
	TaskType{Task: Deactivate{}}.String():            true,
	TaskType{Task: Start{}}.String():                 true,
	TaskType{Task: Stop{}}.String():                  true,
	TaskType{Task: Restart{}}.String():               true,
	TaskType{Task: TransferVersionUpdate{}}.String(): true,
}

type TaskType struct {
	Task
}

func (t TaskType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t TaskType) AsyncSupported() bool {
	return SupportedAsyncOperations[t.String()]
}

func (t *TaskType) UnmarshalJSON(data []byte) error {
	var taskTypeName TaskTypeName
	if err := json.Unmarshal(data, &taskTypeName); err != nil {
		return err
	}
	taskType, ok := TaskTypeByName[taskTypeName]
	if !ok {
		return xerrors.Errorf("Unknown task type: %s", taskTypeName)
	}
	*t = taskType
	return nil
}

func (t TaskType) EncodeText(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	text := pgtype.Text{String: t.String(), Status: pgtype.Present}
	return text.EncodeText(ci, buf)
}

func (t *TaskType) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	var text pgtype.Text
	if err := text.DecodeText(ci, src); err != nil {
		return err
	}
	taskType, ok := TaskTypeByName[text.String]
	if !ok {
		return xerrors.Errorf("Unknown task type: %s", text.String)
	}
	*t = taskType
	return nil
}

func (t TaskType) GobEncode() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *TaskType) GobDecode(data []byte) error {
	key := string(data)
	taskType, ok := TaskTypeByName[key]
	if !ok {
		return xerrors.Errorf("Unknown task type: %s", key)
	}
	*t = taskType
	return nil
}

func (t TaskType) String() string {
	return t.Visit(toStringVisitor{}).(string)
}

func (t TaskType) NewParams() interface{} {
	return t.Visit(paramsVisitor{})
}

func (t TaskType) Description(taskParams interface{}) string {
	return t.Visit(descriptionVisitor{toStringVisitor: toStringVisitor{}, taskParams: taskParams}).(string)
}

type toStringVisitor struct{}

func (toStringVisitor) OnActivate(t Activate) interface{}         { return "Activate" }
func (toStringVisitor) OnUpload(t Upload) interface{}             { return "Upload" }
func (toStringVisitor) OnReUpload(t ReUpload) interface{}         { return "ReUpload" }
func (toStringVisitor) OnStart(t Start) interface{}               { return "Start" }
func (toStringVisitor) OnRestart(t Restart) interface{}           { return "Restart" }
func (toStringVisitor) OnStop(t Stop) interface{}                 { return "Stop" }
func (toStringVisitor) OnVerify(t Verify) interface{}             { return "Verify" }
func (toStringVisitor) OnAddTables(t AddTables) interface{}       { return "AddTables" }
func (toStringVisitor) OnRemoveTables(t RemoveTables) interface{} { return "RemoveTables" }
func (toStringVisitor) OnDeactivate(t Deactivate) interface{}     { return "Deactivate" }
func (toStringVisitor) OnChecksum(t Checksum) interface{}         { return "Checksum" }
func (toStringVisitor) OnReplication(t Replication) interface{}   { return "Replication" }
func (toStringVisitor) OnTermination(t Termination) interface{}   { return "Termination" }
func (toStringVisitor) OnTestEndpoint(t TestEndpoint) interface{} { return "TestEndpoint" }
func (v toStringVisitor) OnUpdateTransfer(t UpdateTransfer) interface{} {
	return "UpdateTransfer"
}

func (toStringVisitor) OnCleanupResource(t CleanupResource) interface{} {
	return "CleanupResource"
}
func (toStringVisitor) OnTransferCreate(t TransferCreate) interface{} {
	return "CreateTransfer"
}
func (toStringVisitor) OnTransferDelete(t TransferDelete) interface{} {
	return "TRANSFER_DELETE"
}
func (toStringVisitor) OnEndpointDelete(t EndpointDelete) interface{} {
	return "ENDPOINT_DELETE"
}
func (toStringVisitor) OnTransferVersionUpdate(t TransferVersionUpdate) interface{} {
	return "Update version"
}
func (toStringVisitor) OnTransferVersionFreeze(t TransferVersionFreeze) interface{} {
	return "Freeze version"
}
func (toStringVisitor) OnTransferVersionUnfreeze(t TransferVersionUnfreeze) interface{} {
	return "Unfreeze version"
}

type paramsVisitor struct{}

func (paramsVisitor) OnActivate(t Activate) interface{}         { return new(interface{}) }
func (paramsVisitor) OnUpload(t Upload) interface{}             { return new(interface{}) }
func (paramsVisitor) OnReUpload(t ReUpload) interface{}         { return new(interface{}) }
func (paramsVisitor) OnStart(t Start) interface{}               { return new(interface{}) }
func (paramsVisitor) OnRestart(t Restart) interface{}           { return new(interface{}) }
func (paramsVisitor) OnStop(t Stop) interface{}                 { return new(interface{}) }
func (paramsVisitor) OnVerify(t Verify) interface{}             { return new(interface{}) }
func (paramsVisitor) OnAddTables(t AddTables) interface{}       { return new(interface{}) }
func (paramsVisitor) OnRemoveTables(t RemoveTables) interface{} { return new(interface{}) }
func (paramsVisitor) OnDeactivate(t Deactivate) interface{}     { return new(interface{}) }
func (paramsVisitor) OnChecksum(t Checksum) interface{}         { return new(interface{}) }
func (paramsVisitor) OnReplication(t Replication) interface{}   { return new(interface{}) }
func (paramsVisitor) OnTermination(t Termination) interface{}   { return new(interface{}) }
func (paramsVisitor) OnTestEndpoint(t TestEndpoint) interface{} { return new(interface{}) }
func (paramsVisitor) OnCleanupResource(t CleanupResource) interface{} {
	return new(interface{})
}
func (v paramsVisitor) OnUpdateTransfer(t UpdateTransfer) interface{} {
	return new(UpdateTransferParams)
}
func (paramsVisitor) OnTransferCreate(t TransferCreate) interface{} { return new(interface{}) }
func (paramsVisitor) OnTransferDelete(t TransferDelete) interface{} { return new(interface{}) }
func (paramsVisitor) OnEndpointDelete(t EndpointDelete) interface{} { return new(interface{}) }

func (paramsVisitor) OnTransferVersionUpdate(t TransferVersionUpdate) interface{} {
	return new(TransferVersionUpdateParams)
}

func (paramsVisitor) OnTransferVersionFreeze(t TransferVersionFreeze) interface{} {
	return new(TransferVersionFreezeParams)
}

func (paramsVisitor) OnTransferVersionUnfreeze(t TransferVersionUnfreeze) interface{} {
	return new(TransferVersionUnfreezeParams)
}

type descriptionVisitor struct {
	toStringVisitor
	taskParams interface{}
}

func (v descriptionVisitor) OnTransferVersionUpdate(t TransferVersionUpdate) interface{} {
	return "Update version"
}

func (v descriptionVisitor) OnTransferVersionFreeze(t TransferVersionFreeze) interface{} {
	return "Freeze version"
}

func (v descriptionVisitor) OnTransferVersionUnfreeze(t TransferVersionUnfreeze) interface{} {
	return "Unfreeze version"
}

func makeTaskTypeByNameMap() map[TaskTypeName]TaskType {
	taskTypeMap := map[TaskTypeName]TaskType{}

	for _, t := range AllTasks {
		mapKey := t.Visit(toStringVisitor{}).(string)
		_, found := taskTypeMap[mapKey]
		if found {
			panic(fmt.Sprintf("Task type name collision at %s", mapKey))
		}
		taskTypeMap[mapKey] = TaskType{t}

		params := t.Visit(paramsVisitor{})
		paramsType := reflect.TypeOf(params)
		if paramsType.Kind() != reflect.Ptr {
			panic("pointer expected")
		}
		dereferencedType := paramsType.Elem()
		zeroValue := reflect.Zero(dereferencedType).Interface()
		if zeroValue == nil {
			continue
		}
		gobwrapper.Register(zeroValue)
	}

	return taskTypeMap
}
