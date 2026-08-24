package s3sess

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

func TestNewAWSSessionDoesNotFollowRedirects(t *testing.T) {
	var redirectedRequests atomic.Int64
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer redirectTarget.Close()

	redirectSource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	defer redirectSource.Close()

	sess, err := NewAWSSession(nil, "bucket", s3_model.ConnectionConfig{Region: "region"})
	require.NoError(t, err, "failed to create AWS session")

	resp, err := sess.Config.HTTPClient.Get(redirectSource.URL)
	require.NoError(t, err, "request failed")
	defer resp.Body.Close()

	require.Equal(t, http.StatusFound, resp.StatusCode, "unexpected response status")
	require.Zero(t, redirectedRequests.Load(), "redirect target received requests")
}
