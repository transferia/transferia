// Package federationmock provides a local gRPC server for testing federated YDB topic readers.
package federationmock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_FederationDiscovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_FederationDiscovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// Config controls discovery and topic streaming. Configure it before connecting clients.
type Config struct {
	Consumer                 string
	Topic                    string
	FederationDatabasePrefix string
	SingleDatabase           bool
	Terminal                 bool // Return SCHEME_ERROR when initializing a read session.
	NonFederated             bool // Return Unimplemented from federation discovery.
	SameEndpoint             bool
	OffsetGap                bool
	CorruptBatch             bool
	ReadBarrier              <-chan struct{}
	Reconnect                <-chan struct{} // Fail the first stream per database when released.
	DiscoveryHook            func(context.Context, string) error
}

// Server deliberately reuses topic, partition and session IDs across accounts.
// Each account has databases "one" and "two", starting at offsets 7 and 42.
// Each stream sends one ZSTD-compressed message containing its database path.
type Server struct {
	Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer
	Ydb_FederationDiscovery_V1.UnimplementedFederationDiscoveryServiceServer
	Ydb_Topic_V1.UnimplementedTopicServiceServer
	Config

	commits       chan string
	stops         chan string
	errors        chan error
	started       chan string
	mu            sync.Mutex
	attempts      map[string]int
	endpoint      string
	otherEndpoint string
	activeStreams atomic.Int64
}

var (
	_ Ydb_Discovery_V1.DiscoveryServiceServer                     = (*Server)(nil)
	_ Ydb_FederationDiscovery_V1.FederationDiscoveryServiceServer = (*Server)(nil)
	_ Ydb_Topic_V1.TopicServiceServer                             = (*Server)(nil)
)

// NewServer starts two plaintext local endpoints and registers their cleanup.
func NewServer(t testing.TB, cfg Config) *Server {
	t.Helper()
	s := &Server{
		UnimplementedDiscoveryServiceServer:           Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer{},
		UnimplementedFederationDiscoveryServiceServer: Ydb_FederationDiscovery_V1.UnimplementedFederationDiscoveryServiceServer{},
		UnimplementedTopicServiceServer:               Ydb_Topic_V1.UnimplementedTopicServiceServer{},
		Config:                                        cfg,
		commits:                                       make(chan string, 16),
		stops:                                         make(chan string, 16),
		errors:                                        make(chan error, 16),
		started:                                       make(chan string, 16),
		mu:                                            sync.Mutex{},
		attempts:                                      make(map[string]int),
		endpoint:                                      "",
		otherEndpoint:                                 "",
		activeStreams:                                 atomic.Int64{},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	other, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })
	s.endpoint, s.otherEndpoint = listener.Addr().String(), other.Addr().String()
	server := grpc.NewServer()
	Ydb_Discovery_V1.RegisterDiscoveryServiceServer(server, s)
	Ydb_FederationDiscovery_V1.RegisterFederationDiscoveryServiceServer(server, s)
	Ydb_Topic_V1.RegisterTopicServiceServer(server, s)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	go func() { _ = server.Serve(other) }()
	return s
}

func (s *Server) Endpoint() string {
	return s.endpoint
}

// Commits reports database paths after their commit responses have been sent.
func (s *Server) Commits() <-chan string {
	return s.commits
}

// Stops reports database paths after receiving stop-session confirmations.
func (s *Server) Stops() <-chan string {
	return s.stops
}

// Started reports "<database path>/<attempt>" after start-session confirmations.
func (s *Server) Started() <-chan string {
	return s.started
}

// Errors reports unexpected stream errors, excluding cancellation and reconnects.
func (s *Server) Errors() <-chan error {
	return s.errors
}

func (s *Server) ActiveStreams() int64 {
	return s.activeStreams.Load()
}

func (s *Server) ListEndpoints(context.Context, *Ydb_Discovery.ListEndpointsRequest) (*Ydb_Discovery.ListEndpointsResponse, error) {
	host, port, err := net.SplitHostPort(s.endpoint)
	if err != nil {
		return nil, err
	}
	n, err := strconv.ParseUint(port, 10, 32)
	if err != nil {
		return nil, err
	}
	result, err := anypb.New(&Ydb_Discovery.ListEndpointsResult{Endpoints: []*Ydb_Discovery.EndpointInfo{{Address: host, Port: uint32(n), NodeId: 1}}})
	if err != nil {
		return nil, err
	}
	return &Ydb_Discovery.ListEndpointsResponse{Operation: &Ydb_Operations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: result}}, nil
}

func (s *Server) ListFederationDatabases(ctx context.Context, _ *Ydb_FederationDiscovery.ListFederationDatabasesRequest) (*Ydb_FederationDiscovery.ListFederationDatabasesResponse, error) {
	if s.NonFederated {
		return nil, status.Error(codes.Unimplemented, "single-database server")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("x-ydb-database")
	if len(values) != 1 {
		return nil, status.Error(codes.InvalidArgument, "missing discovery database")
	}
	account := strings.TrimPrefix(values[0], s.FederationDatabasePrefix+"/")
	if s.DiscoveryHook != nil {
		if err := s.DiscoveryHook(ctx, account); err != nil {
			return nil, err
		}
	}
	result := &Ydb_FederationDiscovery.ListFederationDatabasesResult{}
	ids := []string{"one", "two"}
	if s.SingleDatabase {
		ids = ids[:1]
	}
	for _, id := range ids {
		endpoint := s.endpoint
		if id == "two" && !s.SameEndpoint {
			endpoint = s.otherEndpoint
		}
		result.FederationDatabases = append(result.FederationDatabases, &Ydb_FederationDiscovery.DatabaseInfo{
			Id: id, Name: id, Path: "/" + account + "/" + id, Endpoint: endpoint,
			Status: Ydb_FederationDiscovery.DatabaseInfo_AVAILABLE, Weight: 1,
		})
	}
	wrapped, err := anypb.New(result)
	if err != nil {
		return nil, err
	}
	return &Ydb_FederationDiscovery.ListFederationDatabasesResponse{
		Operation: &Ydb_Operations.Operation{Status: Ydb.StatusIds_SUCCESS, Result: wrapped},
	}, nil
}

func (s *Server) StreamRead(stream Ydb_Topic_V1.TopicService_StreamReadServer) (resultErr error) {
	s.activeStreams.Add(1)
	defer s.activeStreams.Add(-1)
	defer func() {
		if resultErr != nil && stream.Context().Err() == nil && !errors.Is(resultErr, io.EOF) &&
			status.Code(resultErr) != codes.Unavailable && status.Code(resultErr) != codes.Canceled {
			select {
			case s.errors <- resultErr:
			default:
			}
		}
	}()
	md, _ := metadata.FromIncomingContext(stream.Context())
	db := md.Get("x-ydb-database")[0]
	s.mu.Lock()
	s.attempts[db]++
	attempt := s.attempts[db]
	s.mu.Unlock()
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	init := request.GetInitRequest()
	if init.GetConsumer() != s.Consumer || len(init.GetTopicsReadSettings()) != 1 || init.TopicsReadSettings[0].Path != s.Topic {
		return fmt.Errorf("unexpected read settings at %s: %v", db, request)
	}
	if s.Terminal {
		return stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{Status: Ydb.StatusIds_SCHEME_ERROR})
	}
	if err := stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_InitResponse{
			InitResponse: &Ydb_Topic.StreamReadMessage_InitResponse{SessionId: "session" + db},
		},
	}); err != nil {
		return err
	}
	offset := int64(7)
	if strings.HasSuffix(db, "/two") {
		offset = 42
	}
	if err := stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_StartPartitionSessionRequest{
			StartPartitionSessionRequest: &Ydb_Topic.StreamReadMessage_StartPartitionSessionRequest{
				PartitionSession: &Ydb_Topic.StreamReadMessage_PartitionSession{PartitionSessionId: 1, Path: s.Topic, PartitionId: 0},
				CommittedOffset:  offset, PartitionOffsets: &Ydb_Topic.OffsetsRange{Start: offset, End: offset + 1},
			},
		},
	}); err != nil {
		return err
	}
	credit, started, sent := false, false, false
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		switch {
		case request.GetReadRequest() != nil:
			credit = true
		case request.GetStartPartitionSessionResponse() != nil:
			started = true
			select {
			case s.started <- fmt.Sprintf("%s/%d", db, attempt):
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		case request.GetCommitOffsetRequest() != nil:
			ranges := request.GetCommitOffsetRequest().CommitOffsets
			if len(ranges) != 1 || ranges[0].PartitionSessionId != 1 || len(ranges[0].Offsets) != 1 || ranges[0].Offsets[0].Start != offset || ranges[0].Offsets[0].End != offset+1 {
				return fmt.Errorf("wrong commit at %s: %v", db, request)
			}
			if err := stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
				Status: Ydb.StatusIds_SUCCESS,
				ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_CommitOffsetResponse{
					CommitOffsetResponse: &Ydb_Topic.StreamReadMessage_CommitOffsetResponse{
						PartitionsCommittedOffsets: []*Ydb_Topic.StreamReadMessage_CommitOffsetResponse_PartitionCommittedOffset{{PartitionSessionId: 1, CommittedOffset: offset + 1}},
					},
				},
			}); err != nil {
				return err
			}
			select {
			case s.commits <- db:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
			if err := stream.Send(&Ydb_Topic.StreamReadMessage_FromServer{
				Status: Ydb.StatusIds_SUCCESS,
				ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_StopPartitionSessionRequest{
					StopPartitionSessionRequest: &Ydb_Topic.StreamReadMessage_StopPartitionSessionRequest{PartitionSessionId: 1, Graceful: true, CommittedOffset: offset + 1},
				},
			}); err != nil {
				return err
			}
		case request.GetStopPartitionSessionResponse() != nil:
			select {
			case s.stops <- db:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		if credit && started && !sent {
			sent = true
			if s.ReadBarrier != nil {
				select {
				case <-s.ReadBarrier:
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
			}
			messageOffset := offset
			if s.OffsetGap {
				messageOffset += 2
			}
			encoder, err := zstd.NewWriter(nil)
			if err != nil {
				return err
			}
			body := encoder.EncodeAll([]byte(db), nil)
			if err := encoder.Close(); err != nil {
				return err
			}
			if s.CorruptBatch {
				body = []byte("not a zstd frame")
			}
			response := &Ydb_Topic.StreamReadMessage_FromServer{
				Status: Ydb.StatusIds_SUCCESS,
				ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_ReadResponse{
					ReadResponse: &Ydb_Topic.StreamReadMessage_ReadResponse{BytesSize: int64(len(body)),
						PartitionData: []*Ydb_Topic.StreamReadMessage_ReadResponse_PartitionData{{PartitionSessionId: 1,
							Batches: []*Ydb_Topic.StreamReadMessage_ReadResponse_Batch{{Codec: int32(Ydb_Topic.Codec_CODEC_ZSTD), ProducerId: "producer" + db,
								MessageData: []*Ydb_Topic.StreamReadMessage_ReadResponse_MessageData{{Offset: messageOffset, SeqNo: 1, Data: body}},
							}},
						}},
					},
				},
			}
			if err := stream.Send(response); err != nil {
				return err
			}
			if s.Reconnect != nil && attempt == 1 {
				select {
				case <-s.Reconnect:
					return status.Error(codes.Unavailable, "test reconnect without stop callback")
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
			}
		}
	}
}
