//go:build !disable_mongo_provider

package mongo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestMongoFatalErrors(t *testing.T) {
	mustFatal := func(code int32) func(t *testing.T) {
		return func(t *testing.T) {
			{
				require.True(t, isFatalMongoCode(xerrors.Errorf("mongo err: %w", mongo.CommandError{Code: code})))
				require.True(t, isFatalMongoCode(mongo.CommandError{Code: code}))
				require.True(t, isFatalMongoCode(&mongo.CommandError{Code: code}))
				require.True(t, isFatalMongoCode(xerrors.Errorf("mongo err: %w", &mongo.CommandError{Code: code})))
			}
		}
	}
	mustNotFatal := func(code int32) func(t *testing.T) {
		return func(t *testing.T) {
			{
				require.False(t, isFatalMongoCode(xerrors.Errorf("mongo err: %w", mongo.CommandError{Code: code})))
				require.False(t, isFatalMongoCode(mongo.CommandError{Code: code}))
				require.False(t, isFatalMongoCode(&mongo.CommandError{Code: code}))
				require.False(t, isFatalMongoCode(xerrors.Errorf("mongo err: %w", &mongo.CommandError{Code: code})))
			}
		}
	}
	t.Run("ChangeStreamFatalErrorCode", mustFatal(ChangeStreamFatalErrorCode))
	t.Run("ChangeStreamHistoryLostCode", mustFatal(ChangeStreamHistoryLostCode))
	t.Run("any other code", mustNotFatal(123))
}

func TestSerialization(t *testing.T) {
	lastTS := map[ParallelizationUnit]primitive.Timestamp{}
	pu1 := MakeParallelizationUnitDatabase("techDB", "slot", "db")
	pu2 := MakeParallelizationUnitOplog("techDB", "slot")
	lastTS[pu1] = primitive.Timestamp{T: uint32(time.Now().Unix()), I: 0}
	lastTS[pu2] = primitive.Timestamp{T: uint32(time.Now().Unix()), I: 1}
	logger.Log.Info("timestamp", log.Any("lastTS", lastTSforJSON(lastTS)))
}
