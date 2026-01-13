package metering

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/config/env"
	"github.com/transferia/transferia/pkg/instanceutil"
)

type MetricSchema string

const (
	DisabledSerialization = MetricSchema("disabled")
	InputRowsSchema       = MetricSchema("datatransfer.data.input.v1")
	OutputRowsSchema      = MetricSchema("datatransfer.data.output.v1")
	ComputeCPUSchema      = MetricSchema("datatransfer.compute.cpu.v1")
	ComputeRAMSchema      = MetricSchema("datatransfer.compute.ram.v1")
	RuntimeCPUSchema      = MetricSchema("datatransfer.runtime.cpu.v1")
	RuntimeRAMSchema      = MetricSchema("datatransfer.runtime.ram.v1")
	OutputRowsCountSchema = MetricSchema("datatransfer.rows.v1")
	AnalyticsSchema       = MetricSchema("analytics")
)

type Metric interface {
	isMetric()
	Reset() MetricState
}

type MetricState interface {
	Serialize(baseOpts *MeteringOpts) (map[MetricSchema][]string, error)
}

type MeteringOpts struct {
	TransferID      string
	TransferType    string
	FolderID        string
	CloudID         string
	SrcType         string
	DstType         string
	DstMdbClusterID string
	OperationID     string
	OperationType   string
	JobIndex        string
	ComputeVMID     string
	YtOperationID   string
	YtJobID         string
	Host            string
	Runtime         abstract.Runtime
	Tags            map[string]interface{}
}

func (rc *MeteringOpts) BaseMetricFieldsWithLabels(startTS, finishTS time.Time, tags []string) map[string]interface{} {
	base := rc.BaseMetricFields(startTS, finishTS, tags)
	labels := map[string]interface{}{}
	labels["src_type"] = rc.SrcType
	labels["dst_type"] = rc.DstType
	labels["transfer_type"] = rc.TransferType
	if rc.OperationID != "" {
		labels["operation_id"] = rc.OperationID
		labels["operation_type"] = rc.OperationType
	}
	base["labels"] = labels
	return base
}

func (rc *MeteringOpts) BaseMetricFields(startTS, finishTS time.Time, tags []string) map[string]interface{} {
	base := map[string]interface{}{
		"id":          rc.genMetricID(startTS, finishTS, tags),
		"cloud_id":    rc.CloudID,
		"folder_id":   rc.FolderID,
		"resource_id": rc.TransferID,
		"source_wt":   time.Now().Unix(),
		"source_id":   rc.ComputeVMID,
		"version":     "v1alpha1",
		"tags":        map[string]interface{}{},
	}
	if rc.ComputeVMID == "" {
		if rc.YtOperationID != "" {
			base["source_id"] = fmt.Sprintf("%v:%v", rc.YtOperationID, rc.YtJobID)
		} else {
			base["source_id"] = rc.Host
		}
	}
	return base
}

func (rc *MeteringOpts) BaseMetricFieldsWithTags(startTS, finishTS time.Time, tags []string) map[string]interface{} {
	base := rc.BaseMetricFields(startTS, finishTS, tags)
	baseTags := base["tags"].(map[string]interface{})
	baseTags["src_type"] = rc.SrcType
	baseTags["dst_type"] = rc.DstType
	baseTags["transfer_type"] = rc.TransferType
	if rc.OperationID != "" {
		baseTags["operation_id"] = rc.OperationID
		baseTags["operation_type"] = rc.OperationType
	}
	return base
}

func (rc *MeteringOpts) genMetricID(startTS, finishTS time.Time, tags []string) string {
	s := fmt.Sprintf("%v:%v:%v:%v:%v", rc.TransferID, rc.JobIndex, startTS, finishTS, strings.Join(tags, ":"))
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func (rc *MeteringOpts) SourceID(schema MetricSchema) string {
	instanceID := rc.getInstanceID()
	s := fmt.Sprintf("%v:%v:%v", rc.TransferID, instanceID, schema)
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func (rc *MeteringOpts) getInstanceID() string {
	if rc.ComputeVMID != "" {
		return rc.ComputeVMID
	} else if rc.YtJobID != "" {
		return rc.YtJobID
	} else {
		host, _ := os.Hostname()
		return host
	}
}

func NewMeteringOpts(transfer *model.Transfer, task *model.TransferOperation) (*MeteringOpts, error) {
	return NewMeteringOptsWithTags(transfer, task, map[string]interface{}{})
}

func NewMeteringOptsWithTags(transfer *model.Transfer, task *model.TransferOperation, runtimeTags map[string]interface{}) (*MeteringOpts, error) {
	dstMDBClusterID := ""
	if transfer.Dst != nil {
		if clusterable, ok := transfer.Dst.(model.Clusterable); ok {
			dstMDBClusterID = clusterable.MDBClusterID()
		}
	}
	var runtime abstract.Runtime
	if task != nil {
		runtime = task.Runtime
	}
	if runtime == nil {
		// suposedly this is a billing of replication without task
		runtime = transfer.ReplicationRuntime
	}
	if runtime == nil {
		runtime = transfer.Runtime
	}
	if runtime == nil {
		return nil, xerrors.New("runtime is required")
	}

	jobIndex := 0
	runtimeUnwrapped, ok := runtime.(abstract.ShardingTaskRuntime)
	if ok {
		jobIndex = runtimeUnwrapped.CurrentJobIndex()
	}

	config := MeteringOpts{
		TransferID:      transfer.ID,
		TransferType:    string(transfer.Type),
		FolderID:        transfer.FolderID,
		CloudID:         transfer.CloudID,
		SrcType:         string(transfer.SrcType()),
		DstType:         string(transfer.DstType()),
		DstMdbClusterID: dstMDBClusterID,
		OperationID:     "",
		OperationType:   "",
		JobIndex:        strconv.Itoa(jobIndex),
		ComputeVMID:     "",
		YtOperationID:   "",
		YtJobID:         "",
		Host:            "",
		Runtime:         runtime,
		Tags:            runtimeTags,
	}

	if task != nil {
		config.OperationID = task.OperationID
		config.OperationType = task.TaskType.String()
	}

	instanceID, _ := instanceutil.GetGoogleCEMetaData(instanceutil.GoogleID, false)
	config.ComputeVMID = instanceID
	config.YtOperationID = os.Getenv("YT_OPERATION_ID")
	config.YtJobID = os.Getenv("YT_JOB_ID")
	if !env.IsTest() {
		config.Host, _ = os.Hostname()
	}

	logger.Log.Infof("Got metering opts for transfer %v with tags %v", transfer.ID, runtimeTags)
	return &config, nil
}
