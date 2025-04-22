package container

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/transferia/transferia/library/go/core/xerrors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type K8sWrapper struct {
	client kubernetes.Interface
}

func NewK8sWrapper() (*K8sWrapper, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, xerrors.Errorf("failed to load in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, xerrors.Errorf("failed to create k8s client: %w", err)
	}
	return &K8sWrapper{client: clientset}, nil
}

func (w *K8sWrapper) Run(ctx context.Context, opts ContainerOpts) (stdout io.ReadCloser, stderr io.ReadCloser, err error) {
	// Convert options to K8s options
	k8sOpts := opts.ToK8sOpts()

	// Create the pod
	pod, err := w.createPod(ctx, k8sOpts)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to create pod: %w", err)
	}

	// Launch a goroutine to monitor pod completion and cleanup
	go func() {
		watchPod := func() {
			timeout := time.After(k8sOpts.Timeout)
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()

			for {
				select {
				case <-ctx.Done():
					// Context cancelled, clean up the pod
					_ = w.client.CoreV1().Pods(k8sOpts.Namespace).Delete(context.Background(), pod.GetName(), metav1.DeleteOptions{})
					return
				case <-timeout:
					// Timeout reached, clean up the pod
					_ = w.client.CoreV1().Pods(k8sOpts.Namespace).Delete(context.Background(), pod.GetName(), metav1.DeleteOptions{})
					return
				case <-tick.C:
					p, err := w.client.CoreV1().Pods(k8sOpts.Namespace).Get(ctx, pod.GetName(), metav1.GetOptions{})
					if err != nil {
						// Error getting pod info, continue monitoring
						continue
					}

					phase := p.Status.Phase
					if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
						// Pod completed, clean up
						_ = w.client.CoreV1().Pods(k8sOpts.Namespace).Delete(context.Background(), pod.GetName(), metav1.DeleteOptions{})
						return
					}
				}
			}
		}

		watchPod()
	}()

	// Set up log streaming options
	logOpts := &corev1.PodLogOptions{
		Container: k8sOpts.ContainerName,
		Follow:    true, // Stream logs as they become available
	}

	// Get logs stream
	req := w.client.CoreV1().Pods(k8sOpts.Namespace).GetLogs(pod.GetName(), logOpts)
	stream, err := req.Stream(ctx)
	if err != nil {
		// Clean up the pod if we can't get logs
		_ = w.client.CoreV1().Pods(k8sOpts.Namespace).Delete(ctx, pod.GetName(), metav1.DeleteOptions{})
		return nil, nil, xerrors.Errorf("failed to stream pod logs: %w", err)
	}

	// Return the stream immediately (non-blocking)
	// Note: stderr is nil for Kubernetes as GetLogs combines stdout and stderr
	return stream, nil, nil
}

func (w *K8sWrapper) RunAndWait(ctx context.Context, opts ContainerOpts) (stdoutBuf *bytes.Buffer, stderrBuf *bytes.Buffer, err error) {
	// 1. Call Run to get the readers
	stdoutReader, _, err := w.Run(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	defer stdoutReader.Close()

	// 2. Create buffers for output
	stdoutBuf = new(bytes.Buffer)
	stderrBuf = new(bytes.Buffer) // Empty buffer for stderr since K8s doesn't provide separate stderr

	// 3. Copy from the reader to the buffer
	_, err = io.Copy(stdoutBuf, stdoutReader)
	if err != nil && err != io.EOF {
		return stdoutBuf, stderrBuf, xerrors.Errorf("error copying pod logs: %w", err)
	}

	return stdoutBuf, stderrBuf, nil
}

func (w *K8sWrapper) Pull(_ context.Context, _ string, _ types.ImagePullOptions) error {
	// No need to pull images in k8s
	return nil
}

func (w *K8sWrapper) getCurrentNamespace() (string, error) {
	b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// Helper method to create a pod
func (w *K8sWrapper) createPod(ctx context.Context, opts K8sOpts) (*corev1.Pod, error) {
	if opts.Namespace == "" {
		ns, err := w.getCurrentNamespace()
		if err != nil {
			ns = "default"
		}
		opts.Namespace = ns
	}

	if opts.PodName == "" {
		opts.PodName = "transferia-runner"
	}

	if opts.ContainerName == "" {
		opts.ContainerName = "runner"
	}

	// Get the current node name from environment variable
	nodeName := os.Getenv("OPERATOR_POD_NODE_NAME")
	if nodeName == "" {
		// Log a warning if NODE_NAME is not set
		fmt.Println("Warning: OPERATOR_POD_NODE_NAME environment variable not set. Pod will be scheduled according to cluster rules.")
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-", opts.PodName),
			Namespace:    opts.Namespace,
		},
		Spec: corev1.PodSpec{
			// FIXME: This is a temporary workaround to the issue of sharing the data volume between the main process and runner pod.
			// Ideally, we should use a shared volume or a different approach to share data.
			NodeName: nodeName,
			Containers: []corev1.Container{
				{
					Name:         opts.ContainerName,
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

	return w.client.CoreV1().Pods(opts.Namespace).Create(ctx, pod, metav1.CreateOptions{})
}

// Legacy method for backward compatibility
func (w *K8sWrapper) RunPod(ctx context.Context, opts K8sOpts) (*bytes.Buffer, error) {
	// Create a ContainerOpts that will convert to the provided K8sOpts
	containerOpts := ContainerOpts{
		Namespace:     opts.Namespace,
		PodName:       opts.PodName,
		ContainerName: opts.ContainerName,
		Image:         opts.Image,
		RestartPolicy: opts.RestartPolicy,
		Command:       opts.Command,
		Args:          opts.Args,
		Timeout:       opts.Timeout,
	}

	// Convert environment variables
	containerOpts.Env = make(map[string]string)
	for _, env := range opts.Env {
		containerOpts.Env[env.Name] = env.Value
	}

	// Run and wait for completion
	stdoutBuf, _, err := w.RunAndWait(ctx, containerOpts)
	if err != nil {
		return nil, err
	}
	return stdoutBuf, nil
}

func NewK8sWrapperFromKubeconfig(kubeconfigPath string) (*K8sWrapper, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, xerrors.Errorf("unable to build kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, xerrors.Errorf("unable to connect to k8s: %w", err)
	}
	return &K8sWrapper{client: clientset}, nil
}
