// Package tables provides SQL transport, metadata reads and table state for snapshot database strategies.
package ch_eo_common

import (
	"context"
	"database/sql"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

type Client struct {
	dstConfig           *clickhouse_model.ChDestination
	logger              log.Logger
	nodesByShard        [][]Node
	replicaFailures     *ReplicaFailures
	replicaFailureGrace time.Duration
}

// ReplicaFailures retains each host's first failed probe across the clients of
// one snapshot operation. Successful probes clear that host's failure time.
type ReplicaFailures struct {
	startTimes sync.Map
}

func NewClient(dstConfig *clickhouse_model.ChDestination, logger log.Logger, nodesByShard [][]Node) *Client {
	return &Client{
		dstConfig:           dstConfig,
		logger:              logger,
		nodesByShard:        cloneNodesByShard(nodesByShard),
		replicaFailures:     &ReplicaFailures{startTimes: sync.Map{}},
		replicaFailureGrace: time.Minute,
	}
}

const (
	unknownSettingCode  = 115
	timeoutExceededCode = 159
	sessionNotFoundCode = 372
	sessionLockedCode   = 373
)

func (s *Client) ExecuteCommandOverHTTP(ctx context.Context, hostName string, sqlStatement string, httpParams url.Values) error {
	if httpParams.Get("query_id") == "" {
		replicaSync := strings.HasPrefix(sqlStatement, "SYSTEM SYNC REPLICA ") || strings.HasPrefix(sqlStatement, "SYSTEM SYNC DATABASE REPLICA ")
		// Replica catch-up can take longer than a probe. Use the caller's context
		// to limit SYNC instead of the one-minute timeout for short commands.
		if !replicaSync {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Minute)
			defer cancel()
		}
		// These probes and waits can be repeated without replaying data or DDL.
		// Never retry session creation with the same ID: a delayed first request
		// could recreate it after fencing and let a late command execute.
		safeProbe := sqlStatement == "SELECT 1 FORMAT Null" && (httpParams.Get("session_id") == "" || httpParams.Get("session_check") == "1")
		if safeProbe || replicaSync {
			for {
				err := s.RetrySafeQuery(
					ctx,
					hostName,
					func() error {
						return s.executeCommandOverHTTPOnce(ctx, hostName, sqlStatement, httpParams)
					},
				)
				if err == nil {
					return nil
				}
				var serverError *clickhouse_go.Exception
				if !replicaSync || !xerrors.As(err, &serverError) || serverError.Code != timeoutExceededCode {
					return xerrors.Errorf("ExecuteCommandOverHTTP: %w", err)
				}
				// SYNC's server timeout ends only the wait. Background replication
				// continues, so wait again on the same replica until the caller cancels.
				s.logger.Info("Still waiting for ClickHouse replica synchronization after server timeout", log.String("host", hostName), log.Error(err))
				if err := WaitOrCancel(ctx, 100*time.Millisecond); err != nil {
					return xerrors.Errorf("ExecuteCommandOverHTTP: %w", err)
				}
			}
		}
	}
	return s.executeCommandOverHTTPOnce(ctx, hostName, sqlStatement, httpParams)
}

func (s *Client) executeCommandOverHTTPOnce(ctx context.Context, hostName string, sqlStatement string, httpParams url.Values) error {
	clickhouseNode, err := s.NodeByHostName(hostName)
	if err != nil {
		return xerrors.Errorf("executeCommandOverHTTPOnce: %w", err)
	}
	if httpParams == nil {
		httpParams = make(url.Values)
	}

	httpParams.Set("wait_end_of_query", "1")
	responseBody, err := clickhouseNode.HTTP.QueryStream(ctx, s.logger, clickhouseNode.Host, sqlStatement, httpParams)
	if err != nil {
		return xerrors.Errorf("executeCommandOverHTTPOnce: %w", classifySnapshotQueryError(sqlStatement, err))
	}
	defer responseBody.Close()
	// A successful HTTP header alone is not a successful DDL. Read through EOF,
	// including errors that ClickHouse can append to a response body.
	responseBytes, err := io.ReadAll(responseBody)
	if err != nil {
		return xerrors.Errorf("executeCommandOverHTTPOnce: %w", err)
	}
	if strings.TrimSpace(string(responseBytes)) != "" {
		return xerrors.Errorf("executeCommandOverHTTPOnce: %w", classifySnapshotQueryError(sqlStatement, httpclient.ParseCHException(string(responseBytes))))
	}
	return nil
}

// Call only for reads, idempotent waits, or session creation with a fresh ID on
// every attempt. An unknown write result must instead be reconciled by
// UUID/session state or discarded with the whole attempt.
func (s *Client) RetrySafeQuery(ctx context.Context, hostName string, query func() error) error {
	for queryAttemptNumber := 1; ; queryAttemptNumber++ {
		if err := ctx.Err(); err != nil {
			return xerrors.Errorf("RetrySafeQuery: %w", err)
		}
		err := query()
		if err == nil {
			return nil
		}
		if !IsRetryableReadError(err) || ctx.Err() != nil || queryAttemptNumber == 3 {
			return xerrors.Errorf("RetrySafeQuery: %w", err)
		}
		s.logger.Warn("Retrying safe ClickHouse snapshot query after a transport error", log.String("host", hostName), log.Int("attempt", queryAttemptNumber), log.Error(err))
		if err := WaitOrCancel(ctx, time.Duration(queryAttemptNumber)*100*time.Millisecond); err != nil {
			return xerrors.Errorf("RetrySafeQuery: %w", err)
		}
	}
}

func (s *Client) ReadRow(ctx context.Context, hostName string, sqlStatement string, queryArgs []any, destinations ...any) error {
	node, err := s.NodeByHostName(hostName)
	if err != nil {
		return xerrors.Errorf("ReadRow: %w", err)
	}
	return s.RetrySafeQuery(
		ctx,
		hostName,
		func() error {
			return node.DB.QueryRowContext(ctx, sqlStatement, queryArgs...).Scan(destinations...)
		},
	)
}

func (s *Client) ReadStrings(ctx context.Context, hostName string, sqlStatement string, queryArgs ...any) ([]string, error) {
	node, err := s.NodeByHostName(hostName)
	if err != nil {
		return nil, xerrors.Errorf("ReadStrings: %w", err)
	}
	var result []string
	err = s.RetrySafeQuery(
		ctx,
		hostName,
		func() error {
			// Never combine a partially received result with the next SELECT.
			result = nil
			rows, err := node.DB.QueryContext(ctx, sqlStatement, queryArgs...)
			if err != nil {
				return xerrors.Errorf("query string rows: %w", err)
			}
			defer rows.Close()

			for {
				if !rows.Next() {
					break
				}
				var value string
				if err := rows.Scan(&value); err != nil {
					return xerrors.Errorf("scan string row: %w", err)
				}
				result = append(result, value)
			}
			return rows.Err()
		},
	)
	if err != nil {
		return nil, xerrors.Errorf("ReadStrings: %w", err)
	}
	return result, nil
}

// FenceSession proves that even a delayed original request cannot execute SQL.
// An absent query_id in system.processes is not such a proof. Replicated DDL
// needs a separate queue barrier after the session is gone; see tableCommands.FenceCommand.
func (s *Client) FenceSession(ctx context.Context, hostName string, sessionID string) error {
	sessionProbeParams := ExistingSessionParams(sessionID)
	sessionProbeParams.Set("close_session", "1")
	for {
		err := s.ExecuteCommandOverHTTP(ctx, hostName, "SELECT 1 FORMAT Null", sessionProbeParams)
		var serverError *clickhouse_go.Exception
		if xerrors.As(err, &serverError) && serverError.Code == sessionNotFoundCode {
			return nil
		}
		// Old servers may reject or ignore close_session. A successful probe
		// without it renews the 60-second idle timeout, so do not keep polling.
		nextProbeDelay := 65 * time.Second
		switch {
		case err == nil && sessionProbeParams.Has("close_session"):
			// Modern ClickHouse closes the session before finalizing the HTTP
			// response. Confirm absence immediately; success alone is not proof
			// because the server can be configured to ignore close_session.
			sessionProbeParams.Del("close_session")
			continue
		case xerrors.As(err, &serverError) && serverError.Code == unknownSettingCode && strings.Contains(serverError.Message, "close_session"):
			sessionProbeParams.Del("close_session")
		case xerrors.As(err, &serverError) && serverError.Code == sessionLockedCode:
			// A rejected acquisition does not renew the session. The command
			// can finish at any moment, so do not add a minute to every wait.
			nextProbeDelay = 100 * time.Millisecond
		case err != nil:
			return xerrors.Errorf("resolve SQL session %s on %s: %w", sessionID, hostName, err)
		}
		if nextProbeDelay > time.Second {
			s.logger.Info("Waiting for ClickHouse SQL session expiration", log.String("host", hostName), log.String("session", sessionID))
		}
		if err := WaitOrCancel(ctx, nextProbeDelay); err != nil {
			return xerrors.Errorf("FenceSession: %w", err)
		}
	}
}

func (s *Client) CheckSessionFencingSupport(ctx context.Context, hostName string) error {
	// A random session must not be created implicitly when session_check=1.
	err := s.ExecuteCommandOverHTTP(ctx, hostName, "SELECT 1 FORMAT Null", ExistingSessionParams(uuid.NewString()))
	var serverError *clickhouse_go.Exception
	if err == nil {
		return xerrors.New("session_check must reject an absent session, but ClickHouse accepted the query")
	}
	if !xerrors.As(err, &serverError) || serverError.Code != sessionNotFoundCode {
		return xerrors.Errorf("CheckSessionFencingSupport: unable to confirm that session_check rejects an absent session: %w", err)
	}
	sessionProbeCommand, err := s.PrepareSessionCommand(ctx, hostName, "SELECT 1 FORMAT Null")
	if err != nil {
		return xerrors.Errorf("create session probe: %w", err)
	}
	probeCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()

	return s.FenceSession(probeCtx, hostName, sessionProbeCommand.SessionID)
}

func (s *Client) PrepareSessionCommand(ctx context.Context, hostName string, sqlStatement string) (*Command, error) {
	var command *Command
	err := s.RetrySafeQuery(
		ctx,
		hostName,
		func() error {
			sessionID := uuid.NewString()
			// A failed init abandons this ID forever. Even a delayed init is harmless:
			// no modifying command will ever use that session.
			if err := s.ExecuteCommandOverHTTP(ctx, hostName, "SELECT 1 FORMAT Null", url.Values{"session_id": {sessionID}, "session_timeout": {"60"}}); err != nil {
				return xerrors.Errorf("create command session: %w", err)
			}
			command = NewCommand(sessionID, sqlStatement)
			return nil
		},
	)
	if err != nil {
		return nil, xerrors.Errorf("PrepareSessionCommand: %w", err)
	}
	return command, nil
}

func (s *Client) MonitorCommandProgress(ctx context.Context, hostName string, command *Command, cancelHTTPWait context.CancelFunc) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastActiveQueryObservedAt := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		clickhouseNode, err := s.NodeByHostName(hostName)
		if err != nil {
			return
		}
		observationCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var activeQueryCount, readBytes, writtenBytes uint64
		err = s.ReadRow(observationCtx, clickhouseNode.Host.Name, "SELECT count(), sum(read_bytes), sum(written_bytes) FROM system.processes WHERE query_id = ?", []any{command.SessionID}, &activeQueryCount, &readBytes, &writtenBytes)
		cancel()
		if err == nil {
			if activeQueryCount != 0 {
				lastActiveQueryObservedAt = time.Now()
			}
			s.logger.Info("ClickHouse snapshot server query progress", log.String("host", hostName), log.String("query_id", command.SessionID), log.UInt64("active_queries", activeQueryCount), log.UInt64("read_bytes", readBytes), log.UInt64("written_bytes", writtenBytes))
		}
		if time.Since(lastActiveQueryObservedAt) > 3*time.Minute {
			// Lack of observations only interrupts waiting for the HTTP response.
			// executeAndFenceCommand still fences its session before deciding the outcome.
			cancelHTTPWait()
		}
		if command.ReplicaFetchTableName == "" {
			continue
		}
		observationCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		var activeReplicaFetchCount, fetchedCompressedBytes, totalFetchCompressedBytes uint64
		err = s.ReadRow(observationCtx, clickhouseNode.Host.Name, "SELECT count(), sum(bytes_read_compressed), sum(total_size_bytes_compressed) FROM system.replicated_fetches WHERE database = ? AND table = ?", []any{s.dstConfig.Database, command.ReplicaFetchTableName}, &activeReplicaFetchCount, &fetchedCompressedBytes, &totalFetchCompressedBytes)
		cancel()
		if err == nil {
			s.logger.Info("ClickHouse snapshot replica fetch progress", log.String("host", hostName), log.String("table", command.ReplicaFetchTableName), log.UInt64("fetches", activeReplicaFetchCount), log.UInt64("bytes_read", fetchedCompressedBytes), log.UInt64("bytes_total", totalFetchCompressedBytes))
		}
	}
}

func (s *Client) NodeByHostName(hostName string) (*Node, error) {
	for shardIndex := range s.nodesByShard {
		for nodeIndex := range s.nodesByShard[shardIndex] {
			clickhouseNode := &s.nodesByShard[shardIndex][nodeIndex]
			if clickhouseNode.Host.Name == hostName {
				copiedNodes := cloneNodesByShard([][]Node{{*clickhouseNode}})
				return &copiedNodes[0][0], nil
			}
		}
	}
	return nil, xerrors.Errorf("snapshot host %q is absent from destination topology", hostName)
}

func (s *Client) QualifiedTableName(tableName string) string {
	return QuoteIdentifier(s.dstConfig.Database) + "." + QuoteIdentifier(tableName)
}

// TableUUID - returns table 'UUID' by it's name & host
// We need 'TableUUID' to identify tables more reliable than by names
// We then make 'EXCHANGE' - after that need to check who is who
func (s *Client) TableUUID(ctx context.Context, hostName string, tableName string) (string, error) {
	var tableUUID string
	err := s.ReadRow(ctx, hostName, "SELECT toString(uuid) FROM system.tables WHERE database = ? AND name = ?", []any{s.dstConfig.Database, tableName}, &tableUUID)
	if xerrors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", xerrors.Errorf("TableUUID: %w", err)
	}
	return tableUUID, nil
}

func (s *Client) CountPhysicalRows(ctx context.Context, hostName string, tableName string) (uint64, error) {
	var rowCount uint64
	// Count physical rows from active part metadata without scanning table data
	// or relying on the optimizer's trivial-count optimization.
	err := s.ReadRow(ctx, hostName, "SELECT sum(rows) FROM system.parts WHERE active AND database = ? AND table = ?", []any{s.dstConfig.Database, tableName}, &rowCount)
	if err != nil {
		return 0, xerrors.Errorf("CountPhysicalRows: %w", err)
	}
	return rowCount, nil
}

func (s *Client) CheckTableUUID(ctx context.Context, hostName string, tableName string, expectedTableUUID string) error {
	actualTableUUID, err := s.TableUUID(ctx, hostName, tableName)
	if err != nil {
		return xerrors.Errorf("CheckTableUUID: %w", err)
	}
	if actualTableUUID != expectedTableUUID {
		return xerrors.Errorf("snapshot UUID changed on %s/%s: expected %s, found %s", hostName, tableName, expectedTableUUID, actualTableUUID)
	}
	return nil
}

func (s *Client) CheckTableUUIDAndRowCount(ctx context.Context, hostName string, tableName string, expectedTableUUID string, expectedRowCount uint64) error {
	if err := s.CheckTableUUID(ctx, hostName, tableName, expectedTableUUID); err != nil {
		return xerrors.Errorf("CheckTableUUIDAndRowCount: %w", err)
	}
	actualRowCount, err := s.CountPhysicalRows(ctx, hostName, tableName)
	if err != nil {
		return xerrors.Errorf("CheckTableUUIDAndRowCount: %w", err)
	}
	if actualRowCount != expectedRowCount {
		return newRowCountMismatch(hostName, tableName, expectedRowCount, actualRowCount)
	}
	return nil
}

// PartitionIDs - returns all partition IDs in a ClickHouse table so we can copy all its data into the table being built
//
// usually input: tmp_table
// but if 'cleanup=disabled': it's target-table
//
// partitions can be:
//   - cleanup_policy=truncate
//   - cleanup_policy=disabled
//   - cleanup_policy=drop + partition settings
//
// examples of output:
//   - []string{"202601", "202602", "202603"}
//   - []string{"all"} -- if no 'PARTITION BY'
func (s *Client) PartitionIDs(ctx context.Context, hostName string, tableName string) ([]string, error) {
	partitionIDs, err := s.ReadStrings(ctx, hostName, "SELECT DISTINCT partition_id FROM system.parts WHERE active AND database = ? AND table = ? ORDER BY partition_id", s.dstConfig.Database, tableName)
	if err != nil {
		return nil, xerrors.Errorf("PartitionIDs: %w", err)
	}
	return partitionIDs, nil
}

// WaitForMutations waits up to five minutes on this host, respecting earlier cancellation.
func (s *Client) WaitForMutations(ctx context.Context, clickhouseNode Node, tableName string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	for {
		var unfinishedMutationCount uint64
		err := s.ReadRow(waitCtx, clickhouseNode.Host.Name, "SELECT count() FROM system.mutations WHERE database = ? AND table = ? AND NOT is_done", []any{s.dstConfig.Database, tableName}, &unfinishedMutationCount)
		if err != nil {
			if waitCtx.Err() != nil {
				err = waitCtx.Err()
			}
			return xerrors.Errorf("waitForMutations on %s/%s (timeout 5m): %w", clickhouseNode.Host.Name, s.QualifiedTableName(tableName), err)
		}
		if unfinishedMutationCount == 0 {
			return nil
		}
		s.logger.Info("Waiting for ClickHouse mutations", log.String("host", clickhouseNode.Host.Name), log.String("table", tableName), log.UInt64("unfinished_mutations", unfinishedMutationCount))
		if err := WaitOrCancel(waitCtx, 5*time.Second); err != nil {
			return xerrors.Errorf("waitForMutations on %s/%s: %d unfinished mutations (timeout 5m): %w", clickhouseNode.Host.Name, s.QualifiedTableName(tableName), unfinishedMutationCount, err)
		}
	}
}

// JoinedActivePartNames - returns names of physical parts via slash
// Only relevant when cleanup_policy=disabled
//
// example of output: 202601_1_1_0/202601_2_2_0/202602_3_5_1

func (s *Client) JoinedActivePartNames(ctx context.Context, hostName string, tableName string) (string, error) {
	var activePartNames []string
	if err := s.ReadRow(ctx, hostName, "SELECT arraySort(groupArray(name)) FROM system.parts WHERE active AND database = ? AND table = ?", []any{s.dstConfig.Database, tableName}, &activePartNames); err != nil {
		return "", xerrors.Errorf("JoinedActivePartNames: %w", err)
	}
	return strings.Join(activePartNames, "/"), nil
}

// SelectReplicas never treats a SQL/permission/data error as an unavailable
// host. Probe both transports: SQL metadata and HTTP DDL must both be reachable.
// Keep the configured topology intact, including the fixed writer at index zero.

func (s *Client) SelectReplicas(ctx context.Context, skipUnavailableReplicas bool) ([][]Node, error) {
	if !skipUnavailableReplicas {
		return s.NodesByShard(), nil
	}
	reachableByNode := make([][]bool, len(s.nodesByShard))
	var replicaProbes errgroup.Group
	replicaProbes.SetLimit(32)
	for shardIndex, shardNodes := range s.nodesByShard {
		reachableByNode[shardIndex] = make([]bool, len(shardNodes))
		for nodeIndex, node := range shardNodes {
			replicaProbes.Go(
				func() error {
					for {
						if err := ctx.Err(); err != nil {
							return xerrors.Errorf("SelectReplicas: %w", err)
						}
						probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						var probeResult uint8
						err := node.DB.QueryRowContext(probeCtx, "SELECT toUInt8(1)").Scan(&probeResult)
						if err == nil {
							err = s.executeCommandOverHTTPOnce(probeCtx, node.Host.Name, "SELECT 1 FORMAT Null", nil)
						}
						cancel()
						if err == nil {
							s.replicaFailures.startTimes.Delete(node.Host.Name)
							reachableByNode[shardIndex][nodeIndex] = true
							return nil
						}
						if ctx.Err() != nil {
							return xerrors.Errorf("SelectReplicas: %w", ctx.Err())
						}
						if !IsRetryableReadError(err) && !xerrors.Is(err, context.DeadlineExceeded) {
							return xerrors.Errorf("probe snapshot replica %s: %w", node.Host.Name, err)
						}
						firstFailureAt, _ := s.replicaFailures.startTimes.LoadOrStore(node.Host.Name, time.Now())
						if time.Since(firstFailureAt.(time.Time)) >= s.replicaFailureGrace {
							s.logger.Warn("Snapshot replica is unavailable; its DDL will catch up later", log.String("host", node.Host.Name), log.Error(err))
							return nil
						}
						if err := WaitOrCancel(ctx, min(time.Second, s.replicaFailureGrace)); err != nil {
							return xerrors.Errorf("SelectReplicas: %w", err)
						}
					}
				},
			)
		}
	}
	if err := replicaProbes.Wait(); err != nil {
		return nil, xerrors.Errorf("discover snapshot replicas: %w", err)
	}
	availableNodesByShard := make([][]Node, len(s.nodesByShard))
	for shardIndex, shardNodes := range s.nodesByShard {
		for nodeIndex, node := range shardNodes {
			if reachableByNode[shardIndex][nodeIndex] {
				availableNodesByShard[shardIndex] = append(availableNodesByShard[shardIndex], node)
			}
		}
		if len(availableNodesByShard[shardIndex]) == 0 {
			return nil, xerrors.Errorf("snapshot has no available replica for shard %d", shardIndex)
		}
	}
	return cloneNodesByShard(availableNodesByShard), nil
}

// SetReplicaFailureGrace configures how long transport failures are tolerated.
// Configure it before starting replica probes.
func (s *Client) SetReplicaFailureGrace(grace time.Duration) {
	s.replicaFailureGrace = grace
}

// SetReplicaFailures binds clients to their operation's failure history before
// they start probing. The operation owner retains it when a part sink closes.
func (s *Client) SetReplicaFailures(failures *ReplicaFailures) {
	s.replicaFailures = failures
}

// NodesByShard returns an independent topology snapshot. Transport connections
// are shared, but nodes, host descriptions and slice membership are copied.
func (s *Client) NodesByShard() [][]Node {
	return cloneNodesByShard(s.nodesByShard)
}

func cloneNodesByShard(nodesByShard [][]Node) [][]Node {
	if nodesByShard == nil {
		return nil
	}
	copiedNodesByShard := make([][]Node, len(nodesByShard))
	for shardIndex, replicas := range nodesByShard {
		copiedNodesByShard[shardIndex] = append([]Node(nil), replicas...)
		for nodeIndex := range copiedNodesByShard[shardIndex] {
			if replicas[nodeIndex].Host != nil {
				copiedHost := *replicas[nodeIndex].Host
				copiedNodesByShard[shardIndex][nodeIndex].Host = &copiedHost
			}
		}
	}
	return copiedNodesByShard
}
