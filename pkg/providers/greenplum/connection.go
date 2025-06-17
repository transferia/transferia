//go:build !disable_greenplum_provider

package greenplum

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

type GPRole string

type GPSegPointer struct {
	role GPRole
	seg  int
}

func (s GPSegPointer) String() string {
	switch s.role {
	case gpRoleCoordinator:
		return "coordinator"
	case gpRoleSegment:
		return fmt.Sprintf("segment %d", s.seg)
	default:
		panic("improperly initialized GPSegPointer")
	}
}

const (
	gpRoleCoordinator GPRole = "dispatch"
	gpRoleSegment     GPRole = "utility"
)

func Coordinator() GPSegPointer {
	return GPSegPointer{
		role: gpRoleCoordinator,
		seg:  -1,
	}
}

func Segment(index int) GPSegPointer {
	return GPSegPointer{
		role: gpRoleSegment,
		seg:  index,
	}
}

// openPGStorage is a specification of a constructor of PostgreSQL storage for Greenplum.
// May modify the passed storage parameters
func openPGStorage(config *postgres.PgStorageParams) (*postgres.Storage, error) {
	// this creates a TCP connection to the segment!
	var errs util.Errors

	if result, err := postgres.NewStorage(config); err != nil {
		errs = util.AppendErr(errs, err)
	} else {
		return result, nil
	}

	if len(config.TLSFile) > 0 {
		// Try fallback to a connection without TLS.
		// Unfortunately, the TLS error is not a public interface or type; we can only check the message. This is unreliable, so just always try fallback.
		logger.Log.Warn("failed to create a PostgreSQL storage with encrypted connection", log.Error(errs))
		config.TLSFile = ""
		config.TryHostCACertificates = false
		logger.Log.Info("Trying to connect to a PostgreSQL instance using unencrypted connection.")
		if result, err := postgres.NewStorage(config); err != nil {
			errs = util.AppendErr(errs, xerrors.Errorf("fallback to unencrypted connection failed: %w", err))
		} else {
			return result, nil
		}
	}

	return nil, xerrors.Errorf("failed to create a PostgreSQL storage: %w", errs)
}

func (s *Storage) configurePGStorageForGreenplum(storage *postgres.Storage) {
	storage.ForbiddenSchemas = append(storage.ForbiddenSchemas, "gp_toolkit", "mdb_toolkit")
	storage.Flavour = s.newFlavor(s)
}

func (s *Storage) getPgStorageParams(role GPRole) *postgres.PgStorageParams {
	pgs := new(postgres.PgSource)
	pgs.WithDefaults()

	pgs.Database = s.config.Connection.Database
	pgs.User = s.config.Connection.User
	pgs.Password = s.config.Connection.AuthProps.Password
	pgs.DBTables = s.config.IncludeTables
	pgs.ExcludedTables = s.config.ExcludeTables
	pgs.TLSFile = s.config.Connection.AuthProps.CACertificate
	pgs.KeeperSchema = s.config.AdvancedProps.ServiceSchema

	result := pgs.ToStorageParams(nil)

	// force host CA certificates for MDB clusters
	result.TryHostCACertificates = s.config.Connection.MDBCluster != nil

	switch role {
	case gpRoleSegment:
		result.ConnString = "options='-c gp_session_role=utility'"
	default:
		break
	}

	return result
}

// openPGStorageForAnyInPair connects to the current primary of the given high-availability pair AND checks it can execute SQL
func (s *Storage) openPGStorageForAnyInPair(ctx context.Context, sp GPSegPointer) (*postgres.Storage, error) {
	cfg := s.getPgStorageParams(sp.role)
	hap := s.config.Connection.OnPremises.SegByID(sp.seg)

	var errs [2]error
	for i, hp := range []*GpHP{hap.Primary, hap.Mirror} {
		if hp == nil || !hp.Valid() {
			errs[i] = xerrors.New("<missing>")
			continue
		}
		cfg.AllHosts = []string{hp.Host}
		cfg.Port = hp.Port
		logger.Log.Infof("trying to connect to Greenplum %s (%s)", sp.String(), cfg.String())
		result, err := openPGStorage(cfg)
		if err != nil {
			_ = isGPMirrorErr(err, hp.String())
			wrappedErr := xerrors.Errorf("failed to connect to Greenplum %s (%s): %w", sp.String(), cfg.String(), err)
			errs[i] = wrappedErr
			logger.Log.Infof(wrappedErr.Error())
			continue
		}
		s.configurePGStorageForGreenplum(result)
		err = s.checkConnection(ctx, result, sp)
		if err != nil {
			_ = isGPMirrorErr(err, hp.String())
			wrappedErr := xerrors.Errorf("connection to Greenplum %s (%s) is faulty: %w", sp.String(), cfg.String(), err)
			errs[i] = wrappedErr
			logger.Log.Infof(wrappedErr.Error())
			continue
		}
		logger.Log.Infof("successfully connected to Greenplum %s (%s)", sp.String(), cfg.String())
		return result, nil
	}
	return nil, xerrors.Errorf("failed to connect to any host in a highly-availabile pair:\t\t(primary): %v\t\t(mirror): %v", errs[0], errs[1])
}

// checkConnection checks whether the connection in `pgs` is valid (working)
func checkConnection(ctx context.Context, pgs *postgres.Storage, expectedSP GPSegPointer) error {
	conn, err := pgs.Conn.Acquire(ctx)
	if err != nil {
		return xerrors.Errorf("failed to acquire a connection from the pool: %w", err)
	}
	defer conn.Release()

	var gpRole GPRole
	if err := conn.QueryRow(ctx, "SHOW gp_role;").Scan(&gpRole); err != nil {
		return xerrors.Errorf("failed to obtain gp_role: %w", err)
	}
	if err := validateGpRole(expectedSP, gpRole); err != nil {
		return xerrors.Errorf("invalid gp_role: %w", err)
	}

	return nil
}

func validateGpRole(expected GPSegPointer, actual GPRole) error {
	if actual != expected.role {
		return xerrors.Errorf("gp_role %q does not match the expected one %q", actual, expected)
	}
	return nil
}

func segmentsFromGP(ctx context.Context, cpgs *postgres.Storage) ([]*GpHAP, error) {
	conn, err := cpgs.Conn.Acquire(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire a connection from the pool: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, "SELECT content, preferred_role, address, port FROM gp_segment_configuration WHERE content > -1")
	if err != nil {
		return nil, xerrors.Errorf("failed to SELECT data from gp_segment_configuration: %w", err)
	}
	defer rows.Close()
	resultM := make(map[int]*GpHAP)
	for rows.Next() {
		var content int
		var address string
		var port int
		var preferredRole string
		if err := rows.Scan(&content, &preferredRole, &address, &port); err != nil {
			return nil, xerrors.Errorf("failed to scan rows from gp_segment_configuration: %w", err)
		}
		hap := resultM[content]
		if hap == nil {
			hap = new(GpHAP)
		}
		switch preferredRole {
		case "p":
			hap.Primary = NewGpHpWithMDBReplacement(address, port)
		case "m":
			hap.Mirror = NewGpHpWithMDBReplacement(address, port)
		default:
			return nil, abstract.NewFatalError(xerrors.Errorf("unexpected Greenplum preferred_role %q", preferredRole))
		}
		resultM[content] = hap
	}

	result := make([]*GpHAP, len(resultM))
	for k, v := range resultM {
		// in Greenplum, segments are numbered from 0 to (N-1), which corresponds to indexes in the array
		result[k] = v
	}
	return result, nil
}

// isGPMirrorErr checks if the given `err` is due to a connection to a Greenplum instance in recovery mode
func isGPMirrorErr(err error, instanceNameForLog string) bool {
	var pgErr *pgconn.PgError
	if xerrors.As(err, &pgErr) {
		if pgErr.SQLState() == SQLStateInRecovery {
			logger.Log.Infof("Greenplum %s is in recovery mode (this if fine for mirrors)", instanceNameForLog)
			return true
		}
	}
	return false
}

const SQLStateInRecovery string = "57M02"
