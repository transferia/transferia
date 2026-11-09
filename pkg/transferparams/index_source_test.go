package transferparams

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerIndexSources(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		payload string
		index   string
		source  IndexSource
		want    int
	}{
		{name: "ig_main", payload: "instance_name: transfer-1", source: &InstanceNameIndexSource{}, want: 0},
		{name: "ig_secondary", payload: "instance_name: transfer-with-dashes-3", source: &InstanceNameIndexSource{}, want: 2},
		{name: "ig_zero_suffix", payload: "instance_name: transfer-0", source: &InstanceNameIndexSource{}, want: 0},
		{name: "ig_malformed_name", payload: "instance_name: transfer", source: &InstanceNameIndexSource{}, want: 0},
		{name: "ig_invalid_suffix", payload: "instance_name: transfer-invalid", source: &InstanceNameIndexSource{}, want: 0},
		{name: "k8s_main", index: "0", source: &EnvironmentVariableIndexSource{VariableName: "TEST_WORKER_INDEX"}, want: 0},
		{name: "k8s_secondary_is_zero_based", index: "2", source: &EnvironmentVariableIndexSource{VariableName: "TEST_WORKER_INDEX"}, want: 2},
		{name: "dummy", source: &DummyIndexSource{}, want: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			isolateTransferParams(t)
			t.Setenv("TEST_TRANSFER_PARAMS", "spec: original-spec\n"+testCase.payload)
			t.Setenv("TEST_WORKER_INDEX", testCase.index)

			err := Init(&EnvironmentVariableSource{VariableName: "TEST_TRANSFER_PARAMS"}, testCase.source)

			require.NoError(t, err)
			params, err := GetProvider()
			require.NoError(t, err)
			index, err := params.WorkerIndex()
			require.NoError(t, err)
			require.Equal(t, testCase.want, index)
			spec, err := params.Spec()
			require.NoError(t, err)
			require.Equal(t, "original-spec", spec)
		})
	}
}

func TestWorkerIndexInitializationFailureCanRetry(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		value  string
		unset  bool
		source IndexSource
	}{
		{name: "missing_environment_variable", unset: true},
		{name: "empty_environment_variable"},
		{name: "not_an_integer", value: "two"},
		{name: "overflow", value: strings.Repeat("9", 30)},
		{name: "missing_variable_name", source: &EnvironmentVariableIndexSource{}},
		{name: "missing_instance_name", source: &InstanceNameIndexSource{}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			isolateTransferParams(t)
			t.Setenv("TEST_TRANSFER_PARAMS", "spec: original-spec")
			t.Setenv("TEST_WORKER_INDEX", testCase.value)
			if testCase.unset {
				require.NoError(t, os.Unsetenv("TEST_WORKER_INDEX"))
			}
			indexSource := testCase.source
			if indexSource == nil {
				indexSource = &EnvironmentVariableIndexSource{VariableName: "TEST_WORKER_INDEX"}
			}
			source := &EnvironmentVariableSource{VariableName: "TEST_TRANSFER_PARAMS"}

			err := Init(source, indexSource)

			require.Error(t, err)
			params, err := GetProvider()
			require.Error(t, err)
			require.Nil(t, params)
			t.Setenv("TEST_WORKER_INDEX", "2")
			require.NoError(t, Init(source, &EnvironmentVariableIndexSource{VariableName: "TEST_WORKER_INDEX"}))
			params, err = GetProvider()
			require.NoError(t, err)
			index, err := params.WorkerIndex()
			require.NoError(t, err)
			require.Equal(t, 2, index)
		})
	}
}

func TestTransferParamsInitializeOnce(t *testing.T) {
	isolateTransferParams(t)
	t.Setenv("TEST_TRANSFER_PARAMS", "spec: original-spec")
	t.Setenv("TEST_WORKER_INDEX", "2")
	source := &EnvironmentVariableSource{VariableName: "TEST_TRANSFER_PARAMS"}
	indexSource := &EnvironmentVariableIndexSource{VariableName: "TEST_WORKER_INDEX"}
	require.NoError(t, Init(source, indexSource))
	t.Setenv("TEST_TRANSFER_PARAMS", "spec: changed-spec")
	t.Setenv("TEST_WORKER_INDEX", "3")

	require.NoError(t, Init(source, indexSource))

	current, err := GetProvider()
	require.NoError(t, err)
	index, err := current.WorkerIndex()
	require.NoError(t, err)
	require.Equal(t, 2, index)
	spec, err := current.Spec()
	require.NoError(t, err)
	require.Equal(t, "original-spec", spec)
}

func isolateTransferParams(t *testing.T) {
	t.Helper()
	previous := provider
	provider = nil
	t.Cleanup(func() { provider = previous })
}
