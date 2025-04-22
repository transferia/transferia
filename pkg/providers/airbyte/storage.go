package airbyte

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/container"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/math"
	"go.ytsaurus.tech/library/go/core/log"
)

const AirbyteStateKey = "airbyte_state"

var _ abstract.Storage = (*Storage)(nil)

type Storage struct {
	registry metrics.Registry
	cp       coordinator.Coordinator
	logger   log.Logger
	config   *AirbyteSource
	catalog  *Catalog
	metrics  *stats.SourceStats
	transfer *model.Transfer
	state    map[string]*coordinator.TransferStateData

	cw container.ContainerImpl
}

func (a *Storage) Close() {}

func (a *Storage) Ping() error {
	return a.check()
}

func (a *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, pusher abstract.Pusher) error {
	if err := a.check(); err != nil {
		return xerrors.Errorf("unable to check %s table: %w", table.ID().String(), err)
	}
	stream, err := a.configureStream(table.ID())
	if err != nil {
		return xerrors.Errorf("unable to configure stream: %w", err)
	}
	stateJSON := a.extractState(table)
	stateFile := strings.ReplaceAll(fmt.Sprintf("state_%s.json", table.ID().String()), "\"", "")
	if err := a.writeFile(stateFile, stateJSON); err != nil {
		return xerrors.Errorf("unable to write state: %w", err)
	}
	syncCatalogJSON, err := json.Marshal(ConfiguredCatalog{Streams: []ConfiguredStream{*stream}})
	if err != nil {
		return xerrors.Errorf("unable to marshal catalog: %w", err)
	}
	catalogFile := strings.ReplaceAll(fmt.Sprintf("catalog_%s.json", table.ID().String()), "\"", "")
	if err := a.writeFile(catalogFile, string(syncCatalogJSON)); err != nil {
		return xerrors.Errorf("unable to write config: %w", err)
	}
	var lastAirbyteError error
	var currentState json.RawMessage

	args := []string{
		"read",
		"--config",
		"/data/config.json",
		"--state",
		fmt.Sprintf("/data/%s", stateFile),
		"--catalog",
		fmt.Sprintf("/data/%s", catalogFile),
	}

	stdoutReader, stderrReader, err := a.runRawCommand(nil, args...)
	if err != nil {
		return xerrors.Errorf("%s unable to start: %w", table.ID().String(), err)
	}
	defer stdoutReader.Close()
	defer stderrReader.Close()

	var batch *RecordBatch
	cntr := 0
	batch = NewRecordBatch(cntr, stream.Stream.AsModel())

	reader := bufio.NewScanner(stdoutReader)
	buf := make([]byte, 1024*1024, math.Max(a.config.MaxRowSize, 1024*1024))
	reader.Buffer(buf, a.config.MaxRowSize)
	for reader.Scan() {
		select {
		case <-ctx.Done():
			return xerrors.New("load aborted")
		default:
		}
		row := reader.Bytes()
		var r Message
		err := json.Unmarshal(row, &r)
		if err == nil {
			switch r.Type {
			case MessageTypeRecord:
				if batch.size > int(a.config.BatchSizeLimit) || len(batch.records) >= a.config.RecordsLimit || batch.stream.TableID() != r.Record.TableID() {
					items, err := batch.AsChangeItems()
					if err != nil {
						return xerrors.Errorf("unabel to materialize changes: %w", err)
					}
					if err := pusher(items); err != nil {
						return xerrors.Errorf("%s unable to push batch: %w", table.ID().String(), err)
					}
					if err := a.storeState(table.ID(), currentState); err != nil {
						return xerrors.Errorf("%s unable to store incremental state: %w", table.ID().String(), err)
					}
					batch = NewRecordBatch(cntr, stream.Stream.AsModel())
				}
				cntr++
				batch.size += len(row)
				batch.records = append(batch.records, *r.Record)
				a.metrics.ChangeItems.Inc()
				a.metrics.Size.Add(int64(len(row)))
			case MessageTypeLog:
				switch r.Log.Level {
				case "INFO":
					a.logger.Info(r.Log.Message)
				case "WARN":
					a.logger.Warn(r.Log.Message)
				case "ERROR":
					lastAirbyteError = xerrors.New(r.Log.Message)
					a.logger.Error(r.Log.Message)
				case "FATAL":
					lastAirbyteError = xerrors.New(r.Log.Message)
					a.logger.Error(r.Log.Message)
				default:
					a.logger.Infof("%v: %v", r.Log.Level, r.Log.Message)
				}
			case MessageTypeState:
				a.logger.Info("update state", log.Any("state", r.State.Data))
				currentState = r.State.Data
			default:
				a.logger.Infof("line of unknown type: %v: %v", r.Type, string(row))
			}
		} else {
			a.logger.Infof("line: %v", string(row))
		}
	}
	if batch != nil && len(batch.records) > 0 {
		items, err := batch.AsChangeItems()
		if err != nil {
			return xerrors.Errorf("unabel to materialize changes: %w", err)
		}
		if err := pusher(items); err != nil {
			return xerrors.Errorf("%s unable to push last batch: %w", table.ID().String(), err)
		}
	}
	if err := a.storeState(table.ID(), currentState); err != nil {
		return xerrors.Errorf("unable to store incremental state: %w", err)
	}

	// Read stderr to completion to ensure the container process is waited upon
	stderrBuf := new(bytes.Buffer)
	_, err = io.Copy(stderrBuf, stderrReader)
	if err != nil {
		return xerrors.Errorf("%s stderr read failed: %w", table.ID().String(), err)
	}
	if stderrBuf.Len() > 0 {
		a.logger.Warnf("stderr: %v\nlast error:%v", stderrBuf.String(), lastAirbyteError)
	}

	return nil
}

func (a *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	tables, err := a.TableList(nil)
	if err != nil {
		return nil, xerrors.Errorf("unable to list tables: %w", err)
	}
	tableInfo, ok := tables[table]
	if !ok {
		return nil, xerrors.Errorf("table %s not found", table.String())
	}
	return tableInfo.Schema, nil
}

func (a *Storage) TableList(filter abstract.IncludeTableList) (abstract.TableMap, error) {
	if a.catalog == nil {
		if err := a.discover(); err != nil {
			return nil, xerrors.Errorf("unable to discover data objects: %w", err)
		}
	}

	res := abstract.TableMap{}
	for _, stream := range a.catalog.Streams {
		if filter != nil && !filter.Include(stream.TableID()) {
			continue
		}
		tableSchema := a.parseStreamSchema(stream)
		res[stream.TableID()] = abstract.TableInfo{
			EtaRow: 0, // TODO: Integrate https://docs.airbyte.com/understanding-airbyte/airbyte-protocol#airbyteestimatetracemessage estimate message
			IsView: false,
			Schema: abstract.NewTableSchema(tableSchema),
		}
	}
	return res, nil
}

func (a *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, nil
}

func (a *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	// TODO: Integrate https://docs.airbyte.com/understanding-airbyte/airbyte-protocol#airbyteestimatetracemessage estimate message
	return 0, nil
}

func (a *Storage) TableExists(table abstract.TableID) (bool, error) {
	tables, err := a.TableList(nil)
	if err != nil {
		return false, xerrors.Errorf("unable to list tables: %w", err)
	}
	_, ok := tables[table]
	return ok, nil
}

func (a *Storage) configureStream(tid abstract.TableID) (*ConfiguredStream, error) {
	// this need to mimic airbyte native behavior
	if a.catalog == nil {
		if err := a.discover(); err != nil {
			return nil, xerrors.Errorf("unable to discover data objects: %w", err)
		}
	}
	sort.Slice(a.catalog.Streams, func(i, j int) bool {
		return strings.ToLower(a.catalog.Streams[i].Name) < strings.ToLower(a.catalog.Streams[j].Name)
	})
	for _, stream := range a.catalog.Streams {
		if stream.TableID() != tid {
			continue
		}
		var cursorField []string
		mode := "full_refresh"
		if a.transfer.RegularSnapshot != nil {
			for _, t := range a.transfer.RegularSnapshot.Incremental {
				if stream.TableID() == tid {
					cursorField = []string{t.CursorField}
					if t.CursorField == "" {
						cursorField = stream.DefaultCursorField
					}
					mode = "incremental"
				}
			}
		} else if !a.transfer.SnapshotOnly() {
			mode = "incremental"
			cursorField = stream.DefaultCursorField
		}
		if !stream.SupportMode(mode) {
			return nil, xerrors.Errorf("stream: %s not support mode: %s, supported modes: %s", stream.Name, mode, stream.SupportedSyncModes)
		}
		return &ConfiguredStream{
			Stream:              stream,
			SyncMode:            mode,
			DestinationSyncMode: DestinationSyncModeAppend,
			CursorField:         cursorField,
			PrimaryKey:          stream.SourceDefinedPrimaryKey,
		}, nil
	}
	return nil, xerrors.Errorf("unable to found part: %s in catalog", tid.String())
}

func (a *Storage) parseStreamSchema(stream Stream) abstract.TableColumns {
	keys := map[string]bool{}
	for _, keyRow := range stream.SourceDefinedPrimaryKey {
		for _, colName := range keyRow {
			keys[colName] = true
		}
	}
	tableSchema := toSchema(stream.ParsedJSONSchema(), keys)
	if len(keys) == 0 {
		tableSchema = append(tableSchema, RecordIndexCol)
	}
	return tableSchema
}

func (a *Storage) parse(data []byte) (*Message, []string) {
	var logs []string
	var res *Message
	scanner := bufio.NewScanner(bytes.NewReader(data))
	buf := make([]byte, 1024*1024, math.Max(1024*1024, a.config.MaxRowSize))
	scanner.Buffer(buf, math.Max(1024*1024, a.config.MaxRowSize))
	for scanner.Scan() {
		row := scanner.Bytes()
		if len(row) > 1024*1024 {
			a.logger.Warnf("large row: %s, snippet: \n%s", format.SizeInt(len(row)), util.Sample(string(row), 256))
		}
		var r Message
		err := json.Unmarshal(row, &r)
		if err != nil {
			a.logger.Debugf("row: %v, err: %v", string(row), err)
			logs = append(logs, string(row))
		} else if r.Type == MessageTypeLog {
			logs = append(logs, r.Log.Message)
		} else {
			res = &r
		}
	}
	return res, logs
}

func (a *Storage) writeFile(fileName, fileData string) error {
	fullPath := filepath.Join(a.config.DataDir(), fileName)
	a.logger.Debugf("%s -> \n%s", fileName, fileData)
	defer a.logger.Infof("file(%s) %s written", format.SizeInt(len(fileData)), fullPath)
	return os.WriteFile(
		fullPath,
		[]byte(fileData),
		0o664,
	)
}

func (a *Storage) check() error {
	a.logger.Infof("begin check")
	if err := a.writeFile("config.json", a.config.Config); err != nil {
		return xerrors.Errorf("unable to write config: %w", err)
	}

	configResponse, err := a.runCommand("check", "--config", "/data/config.json")
	if err != nil {
		return err
	}
	resp, logs := a.parse(configResponse)
	for _, row := range logs {
		a.logger.Infof("config: %v", row)
	}

	if resp == nil {
		return xerrors.New("empty response")
	}

	if resp.Type != MessageTypeConnectionStatus {
		return xerrors.Errorf("unexpected response type: %v", resp.Type)
	}
	if resp.ConnectionStatus == nil {
		return xerrors.New("empty connection status")
	}
	if resp.ConnectionStatus.Status != "SUCCEEDED" {
		return xerrors.Errorf("unexpected connection status: %v: %v", resp.ConnectionStatus.Status, resp.ConnectionStatus.Message)
	}
	return nil
}

func (a *Storage) discover() error {
	if err := a.check(); err != nil {
		return xerrors.Errorf("unable to check provider: %w", err)
	}
	response, err := a.runCommand("discover", "--config", "/data/config.json")
	if err != nil {
		return xerrors.Errorf("exec error: %w", err)
	}
	resp, logs := a.parse(response)
	for _, row := range logs {
		a.logger.Infof("config: %v", row)
	}
	if resp == nil {
		return xerrors.New("empty catalog")
	}
	if resp.Type != MessageTypeCatalog {
		return xerrors.Errorf("unexpected response type: %v", resp.Type)
	}
	if resp.Catalog == nil || len(resp.Catalog.Streams) == 0 {
		return xerrors.New("resolved catalog empty")
	}
	a.catalog = resp.Catalog
	return nil
}

func (a *Storage) baseOpts() container.ContainerOpts {
	return container.ContainerOpts{
		Env: map[string]string{
			"AWS_EC2_METADATA_DISABLED": "true",
		},
		LogOptions: map[string]string{
			"max-size": "100m",
			"max-file": "3",
		},
		Namespace:     "",
		RestartPolicy: "Never",
		PodName:       "airbyte-runner",
		Image:         a.config.DockerImage(),
		LogDriver:     "local",
		Network:       "host",
		ContainerName: "runner",
		Volumes: []container.Volume{
			{
				Name:          "data",
				HostPath:      a.config.DataDir(),
				ContainerPath: "/data",
				VolumeType:    "bind",
			},
		},
		Command: nil,
		Args:    nil,
		// FIXME: make this configurable
		Timeout:      12 * time.Hour,
		AttachStdout: true,
		AttachStderr: true,
		AutoRemove:   true,
	}
}

func (a *Storage) runRawCommand(cmd []string, args ...string) (io.ReadCloser, io.ReadCloser, error) {
	ctx := context.Background()

	opts := a.baseOpts()

	opts.Command = cmd
	opts.Args = args

	a.logger.Info(opts.String())

	return a.cw.Run(ctx, opts)
}

func (a *Storage) runCommand(args ...string) ([]byte, error) {
	// 1. Call runRawCommand to get the readers
	stdoutReader, stderrReader, cmdErr := a.runRawCommand(nil, args...)
	if cmdErr != nil {
		opts := a.baseOpts()
		opts.Command = args
		a.logger.Error(cmdErr.Error())
		return nil, xerrors.Errorf("command failed: %w", cmdErr)
	}
	defer stdoutReader.Close()
	defer stderrReader.Close()

	// 2. Create buffers for output
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	// 3. Use WaitGroup to wait for both streams to be copied
	var wg sync.WaitGroup
	wg.Add(2)

	// 4. Channel for collecting errors
	errCh := make(chan error, 2)

	// Read stdout line by line
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			// Append each line with a newline character
			stdoutBuf.Write(scanner.Bytes())
			stdoutBuf.WriteByte('\n')
		}
		if err := scanner.Err(); err != nil {
			errCh <- xerrors.Errorf("error scanning stdout: %w", err)
		}
	}()

	// Read stderr line by line
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrReader)
		for scanner.Scan() {
			// Append each line with a newline character
			stderrBuf.Write(scanner.Bytes())
			stderrBuf.WriteByte('\n')
		}
		if err := scanner.Err(); err != nil {
			errCh <- xerrors.Errorf("error scanning stderr: %w", err)
		}
	}()

	// 5. Wait for both copies to complete
	wg.Wait()
	close(errCh)

	// 6. Collect any errors
	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}

	// 7. Return combined error if any
	if len(errs) > 0 {
		var combinedErr error
		for _, e := range errs {
			if combinedErr == nil {
				combinedErr = e
			} else {
				combinedErr = xerrors.Errorf("%v; %w", combinedErr, e)
			}
		}
		return nil, combinedErr
	}

	// Scan the captured stderrBuf for stderr messages
	if stderrBuf.Len() > 0 {
		scanner := bufio.NewScanner(bytes.NewReader(stderrBuf.Bytes()))
		var stderrErrs util.Errors
		for scanner.Scan() {
			stderrErrs = append(stderrErrs, xerrors.New(scanner.Text()))
		}
		if len(stderrErrs) > 0 {
			a.logger.Warnf("stderr: %v", log.Error(stderrErrs))
		}
	}

	return stdoutBuf.Bytes(), nil
}

func (a *Storage) extractState(table abstract.TableDescription) string {
	if table.Filter != "" {
		a.logger.Info("read from state", log.Any("table", table.Fqtn()), log.Any("state", table.Filter))
		return string(table.Filter)
	}
	a.logger.Info("empty state", log.Any("table", table.Fqtn()))
	return `{
	"cdc": false
}`
}

func StateKey(table abstract.TableID) string {
	return fmt.Sprintf("%s_%s", AirbyteStateKey, table.Fqtn())
}

func (a *Storage) storeState(id abstract.TableID, state json.RawMessage) error {
	if state != nil {
		a.logger.Info("save state", log.Any("table", id.Fqtn()), log.Any("state", state))
		if err := a.cp.SetTransferState(
			a.transfer.ID,
			map[string]*coordinator.TransferStateData{
				StateKey(id): {Generic: state},
			},
		); err != nil {
			return xerrors.Errorf("unable to set transfer state: %w", err)
		}
	}
	return nil
}

func NewStorage(lgr log.Logger, registry metrics.Registry, cp coordinator.Coordinator, cfg *AirbyteSource, transfer *model.Transfer) (*Storage, error) {
	state, err := cp.GetTransferState(transfer.ID)
	if err != nil {
		return nil, xerrors.Errorf("unable to extract transfer state: %w", err)
	}
	if len(state) > 0 {
		lgr.Info("airbyte storage constructed with state", log.Any("state", state))
	}

	containerImpl, err := container.NewContainerImpl(lgr)
	if err != nil {
		return nil, xerrors.Errorf("unable to ensure dockerd running, please ensure you have specified supervisord with it: %w", err)
	}

	return &Storage{
		registry: registry,
		cp:       cp,
		logger:   lgr,
		config:   cfg,
		catalog:  nil,
		metrics:  stats.NewSourceStats(registry),
		transfer: transfer,
		state:    state,
		cw:       containerImpl,
	}, nil
}
