package functions

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
)

func TestRedirectsForbidden(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1/test", http.StatusTemporaryRedirect)
	}))
	opts := &model.DataTransformOptions{CloudFunction: "whatever"}
	baseURL := "http://" + testServer.Listener.Addr().String()
	executor, err := NewExecutor(opts, baseURL, YDS, logger.Log, solomon.NewRegistry(solomon.NewRegistryOpts()))
	require.NoError(t, err)

	_, err = executor.Do([]abstract.ChangeItem{{
		Kind:         abstract.InsertKind,
		Schema:       "s",
		Table:        "t",
		ColumnNames:  abstract.RawDataSchema.ColumnNames(),
		ColumnValues: []any{"topic", uint32(0), uint64(0), time.Time{}, "kek"},
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no redirects are allowed")
}
