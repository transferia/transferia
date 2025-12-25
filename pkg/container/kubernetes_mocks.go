package container

import (
	"context"
	"io"

	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MockKubernetesClient struct {
	mock.Mock
}

func (m *MockKubernetesClient) CreatePod(ctx context.Context, namespace string, pod *corev1.Pod) (*corev1.Pod, error) {
	args := m.Called(ctx, namespace, pod)
	return args.Get(0).(*corev1.Pod), args.Error(1)
}

func (m *MockKubernetesClient) GetPodLogs(ctx context.Context, namespace, podName, containerName string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	args := m.Called(ctx, namespace, podName, containerName, opts)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *MockKubernetesClient) GetPod(ctx context.Context, namespace, podName string) (*corev1.Pod, error) {
	args := m.Called(ctx, namespace, podName)
	return args.Get(0).(*corev1.Pod), args.Error(1)
}

func (m *MockKubernetesClient) DeletePod(ctx context.Context, namespace, podName string, opts *metav1.DeleteOptions) error {
	args := m.Called(ctx, namespace, podName, opts)
	return args.Error(0)
}
