package container

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

type K8sWrapperTest struct {
	client KubernetesClient
}

func (w *K8sWrapperTest) RunPod(ctx context.Context, opts K8sOpts) (stdout bytes.Buffer, err error) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: opts.PodName,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:         opts.PodName,
					Image:        opts.Image,
					Command:      opts.Command,
					Args:         opts.Args,
					Env:          opts.Env,
					VolumeMounts: opts.VolumeMounts,
				},
			},
			Volumes:       opts.Volumes,
			RestartPolicy: opts.RestartPolicy,
		},
	}

	_, err = w.client.CreatePod(ctx, opts.Namespace, pod)
	if err != nil {
		return stdout, fmt.Errorf("failed to create pod: %w", err)
	}

	timeout := time.After(opts.Timeout)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
waitingLoop:
	for {
		select {
		case <-timeout:
			_ = w.client.DeletePod(ctx, opts.Namespace, opts.PodName, &metav1.DeleteOptions{})
			return stdout, fmt.Errorf("timeout waiting for pod %s to complete", opts.PodName)
		case <-tick.C:
			p, err := w.client.GetPod(ctx, opts.Namespace, opts.PodName)
			if err != nil {
				return stdout, fmt.Errorf("failed to get pod info: %w", err)
			}
			if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
				break waitingLoop
			}
		}
	}

	logOpts := &corev1.PodLogOptions{
		Container: opts.PodName,
	}
	rc, err := w.client.GetPodLogs(ctx, opts.Namespace, opts.PodName, opts.PodName, logOpts)
	if err != nil {
		return stdout, fmt.Errorf("failed to get pod logs: %w", err)
	}
	defer rc.Close()

	_, err = io.Copy(&stdout, rc)
	if err != nil {
		return stdout, fmt.Errorf("failed copying pod logs: %w", err)
	}

	_ = w.client.DeletePod(ctx, opts.Namespace, opts.PodName, &metav1.DeleteOptions{})
	return stdout, nil
}

func TestK8sOptsString(t *testing.T) {
	opts := K8sOpts{
		Namespace:     "default",
		PodName:       "example-pod",
		Image:         "nginx:latest",
		RestartPolicy: corev1.RestartPolicyAlways,
		Command:       []string{"nginx"},
		Args:          []string{"-g", "daemon off;"},
		Env: []corev1.EnvVar{
			{
				Name:  "ENV_VAR",
				Value: "value",
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "data",
				MountPath: "/data",
			},
		},
		Timeout: 30 * time.Second,
	}

	yamlData := opts.String()

	var pod corev1.Pod
	if err := yaml.Unmarshal([]byte(yamlData), &pod); err != nil {
		t.Fatalf("Unmarshalling pod YAML failed: %v", err)
	}

	// Verify that the pod metadata matches.
	if pod.ObjectMeta.Name != opts.PodName {
		t.Errorf("Expected pod name %q, got %q", opts.PodName, pod.ObjectMeta.Name)
	}
	if pod.ObjectMeta.Namespace != opts.Namespace {
		t.Errorf("Expected namespace %q, got %q", opts.Namespace, pod.ObjectMeta.Namespace)
	}

	// Verify restart policy.
	if pod.Spec.RestartPolicy != opts.RestartPolicy {
		t.Errorf("Expected restart policy %q, got %q", opts.RestartPolicy, pod.Spec.RestartPolicy)
	}

	// Verify that there's one container and its fields match.
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("Expected exactly one container, got %d", len(pod.Spec.Containers))
	}

	container := pod.Spec.Containers[0]
	if container.Image != opts.Image {
		t.Errorf("Expected image %q, got %q", opts.Image, container.Image)
	}
	if len(container.Command) != len(opts.Command) {
		t.Errorf("Expected %d command(s), got %d", len(opts.Command), len(container.Command))
	}
	for i, v := range container.Command {
		if v != opts.Command[i] {
			t.Errorf("Expected command at index %d to be %q, got %q", i, opts.Command[i], v)
		}
	}
	if len(container.Args) != len(opts.Args) {
		t.Errorf("Expected %d arg(s), got %d", len(opts.Args), len(container.Args))
	}
	for i, v := range container.Args {
		if v != opts.Args[i] {
			t.Errorf("Expected arg at index %d to be %q, got %q", i, opts.Args[i], v)
		}
	}

	// Verify environment variables.
	if len(container.Env) != len(opts.Env) {
		t.Errorf("Expected %d environment variables, got %d", len(opts.Env), len(container.Env))
	} else {
		for i, envVar := range container.Env {
			if envVar.Name != opts.Env[i].Name || envVar.Value != opts.Env[i].Value {
				t.Errorf("Expected env var at index %d to be %+v, got %+v", i, opts.Env[i], envVar)
			}
		}
	}

	// Verify VolumeMounts in container.
	if len(container.VolumeMounts) != len(opts.VolumeMounts) {
		t.Errorf("Expected %d volumeMount(s), got %d", len(opts.VolumeMounts), len(container.VolumeMounts))
	} else {
		for i, vm := range container.VolumeMounts {
			if vm.Name != opts.VolumeMounts[i].Name || vm.MountPath != opts.VolumeMounts[i].MountPath {
				t.Errorf("Expected volumeMount at index %d to be %+v, got %+v", i, opts.VolumeMounts[i], vm)
			}
		}
	}

	// Verify Volumes at pod spec level.
	if len(pod.Spec.Volumes) != len(opts.Volumes) {
		t.Errorf("Expected %d volume(s), got %d", len(opts.Volumes), len(pod.Spec.Volumes))
	} else {
		for i, vol := range pod.Spec.Volumes {
			if vol.Name != opts.Volumes[i].Name {
				t.Errorf("Expected volume at index %d to have name %q, got %q", i, opts.Volumes[i].Name, vol.Name)
			}
		}
	}
}

func TestK8sOptsString_MarshalError(t *testing.T) {
	opts := K8sOpts{
		Namespace:     "default",
		PodName:       "test-pod",
		Image:         "busybox:latest",
		RestartPolicy: corev1.RestartPolicyOnFailure,
		Command:       []string{"sleep"},
		Args:          []string{"3600"},
		Env:           []corev1.EnvVar{},
		Volumes:       []corev1.Volume{},
		VolumeMounts:  []corev1.VolumeMount{},
		Timeout:       10 * time.Second,
	}

	yamlStr := opts.String()
	if yamlStr == "" {
		t.Error("Expected a non-empty YAML string")
	}
	var pod corev1.Pod
	if err := yaml.Unmarshal([]byte(yamlStr), &pod); err != nil {
		t.Errorf("Expected valid YAML, but got error: %v", err)
	}
}

func TestK8sWrapper_RunPod_Success(t *testing.T) {
	mockClient := new(MockKubernetesClient)
	wrapper := &K8sWrapperTest{client: mockClient}
	ctx := context.Background()
	opts := K8sOpts{
		Namespace:     "default",
		PodName:       "test-pod",
		Image:         "alpine",
		Command:       []string{"echo", "Hello, Kubernetes!"},
		Timeout:       3 * time.Second,
		RestartPolicy: corev1.RestartPolicyNever,
	}

	// Expected pod returned from CreatePod.
	createdPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: opts.PodName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	// Setup mock expectations.
	mockClient.On("CreatePod", ctx, opts.Namespace, mock.AnythingOfType("*v1.Pod")).Return(createdPod, nil).Once()

	// Simulate GetPod returning a succeeded pod.
	succeededPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: opts.PodName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}
	// During polling, GetPod will be called repeatedly. For simplicity, always return the succeeded pod.
	mockClient.On("GetPod", ctx, opts.Namespace, opts.PodName).Return(succeededPod, nil).Maybe()

	// Setup GetPodLogs to return a reader with log content.
	logContent := "Pod execution log\n"
	mockClient.On("GetPodLogs", ctx, opts.Namespace, opts.PodName, opts.PodName, mock.AnythingOfType("*v1.PodLogOptions")).Return(io.NopCloser(strings.NewReader(logContent)), nil).Once()

	// Setup DeletePod expectation.
	mockClient.On("DeletePod", ctx, opts.Namespace, opts.PodName, mock.Anything).Return(nil).Maybe()

	stdout, err := wrapper.RunPod(ctx, opts)
	assert.NoError(t, err)
	assert.Equal(t, logContent, stdout.String())

	mockClient.AssertExpectations(t)
}

func TestK8sWrapper_RunPod_Timeout(t *testing.T) {
	mockClient := new(MockKubernetesClient)
	wrapper := &K8sWrapperTest{client: mockClient}
	ctx := context.Background()
	opts := K8sOpts{
		Namespace:     "default",
		PodName:       "timeout-pod",
		Image:         "alpine",
		Command:       []string{"sleep", "100"},
		Timeout:       1 * time.Second,
		RestartPolicy: corev1.RestartPolicyNever,
	}

	createdPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: opts.PodName,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
	mockClient.On("CreatePod", ctx, opts.Namespace, mock.AnythingOfType("*v1.Pod")).Return(createdPod, nil).Once()
	// Always return the pending pod.
	mockClient.On("GetPod", ctx, opts.Namespace, opts.PodName).Return(createdPod, nil).Maybe()
	// DeletePod is expected to be called when timing out.
	mockClient.On("DeletePod", ctx, opts.Namespace, opts.PodName, mock.Anything).Return(nil).Once()

	stdout, err := wrapper.RunPod(ctx, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.Equal(t, "", stdout.String())

	mockClient.AssertExpectations(t)
}

func TestIntegration_RunPod(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if os.Getenv("TEST_KUBERNETES_INTEGRATION") == "" {
		t.Skip("Skipping integration test. To run, set TEST_KUBERNETES_INTEGRATION=1")
	}

	kindClusterName := "integration-test-cluster"

	// Create a new Kind cluster.
	createCmd := exec.Command("kind", "create", "cluster", "--name", kindClusterName)
	createOut, err := createCmd.CombinedOutput()
	require.NoError(t, err, "Failed to create Kind cluster: %s", string(createOut))

	// Ensure that we delete the cluster upon test completion.
	defer func() {
		deleteCmd := exec.Command("kind", "delete", "cluster", "--name", kindClusterName)
		deleteOut, err := deleteCmd.CombinedOutput()
		if err != nil {
			t.Logf("Failed to delete Kind cluster: %s. Output: %s", err, string(deleteOut))
		}
	}()

	// Wait a bit for the cluster to be fully up.
	time.Sleep(15 * time.Second)

	// Retrieve the kubeconfig for the new Kind cluster.
	kubeconfigCmd := exec.Command("kind", "get", "kubeconfig", "--name", kindClusterName)
	kubeconfigData, err := kubeconfigCmd.Output()
	require.NoError(t, err, "Failed to retrieve kubeconfig for Kind cluster")

	tmpFile, err := os.CreateTemp("", "kind-kubeconfig-")
	require.NoError(t, err, "Failed to create temporary kubeconfig file")
	_, err = tmpFile.Write(kubeconfigData)
	require.NoError(t, err, "Failed to write kubeconfig data to file")
	err = tmpFile.Close()
	require.NoError(t, err, "Failed to close temporary kubeconfig file")
	defer os.Remove(tmpFile.Name())

	// Create a new Kubernetes client using the test kubeconfig.
	wrapper, err := NewK8sWrapperFromKubeconfig(tmpFile.Name())
	require.NoError(t, err, "Failed to create K8sWrapper from kubeconfig")

	opts := K8sOpts{
		Namespace:     "default",
		PodName:       "integration-pod",
		Image:         "busybox", // Using busybox as a lightweight image.
		Command:       []string{"sh", "-c"},
		Args:          []string{"echo Integration test successful"},
		RestartPolicy: corev1.RestartPolicyNever,
		Timeout:       60 * time.Second,
	}

	ctx := context.Background()
	stdout, err := wrapper.RunPod(ctx, opts)
	require.NoError(t, err, "Failed to run pod in integration test")

	// Read the pod's output.
	outputData, err := io.ReadAll(stdout)
	require.NoError(t, err, "Failed to read pod output")

	expectedOutput := "Integration test successful"
	require.Contains(t, string(outputData), expectedOutput, "Pod output did not contain expected message")
}
