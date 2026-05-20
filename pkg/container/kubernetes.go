package container

import (
	"bytes"
	"context"
	"io"
	"time"

	docker_image "github.com/docker/docker/api/types/image"
	"github.com/transferia/transferia/library/go/core/xerrors"
	k8s_api "k8s.io/api/core/v1"
	k8s_api_meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	k8s_rest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type K8sWrapper struct {
	client kubernetes.Interface
}

func NewK8sWrapper() (*K8sWrapper, error) {
	config, err := k8s_rest.InClusterConfig()
	if err != nil {
		return nil, xerrors.Errorf("failed to load in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, xerrors.Errorf("failed to create k8s client: %w", err)
	}
	return &K8sWrapper{client: clientset}, nil
}

func (w *K8sWrapper) Run(ctx context.Context, opts ContainerOpts) (io.Reader, io.Reader, error) {
	// Unfortunately, Kubernetes does not provide a way to demux stdout and stderr
	b, err := w.RunPod(ctx, opts.ToK8sOpts())

	return b, nil, err
}

func (w *K8sWrapper) Pull(_ context.Context, _ string, _ docker_image.PullOptions) error {
	// No need to pull images in k8s
	return nil
}

func (w *K8sWrapper) RunPod(ctx context.Context, opts K8sOpts) (*bytes.Buffer, error) {
	pod := &k8s_api.Pod{
		ObjectMeta: k8s_api_meta.ObjectMeta{
			Name: opts.PodName,
		},
		Spec: k8s_api.PodSpec{
			Containers: []k8s_api.Container{
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

	_, err := w.client.CoreV1().Pods(opts.Namespace).Create(ctx, pod, k8s_api_meta.CreateOptions{})
	if err != nil {
		return nil, xerrors.Errorf("failed to create pod: %w", err)
	}

	timeout := time.After(opts.Timeout)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

waitLoop:
	for {
		select {
		case <-timeout:
			// If timed out, clean up.
			_ = w.client.CoreV1().Pods(opts.Namespace).Delete(ctx, opts.PodName, k8s_api_meta.DeleteOptions{})
			return nil, xerrors.Errorf("timeout waiting for pod %s to complete", opts.PodName)
		case <-tick.C:
			p, err := w.client.CoreV1().Pods(opts.Namespace).Get(ctx, opts.PodName, k8s_api_meta.GetOptions{})
			if err != nil {
				return nil, xerrors.Errorf("failed to get pod info: %w", err)
			}
			phase := p.Status.Phase
			if phase == k8s_api.PodSucceeded || phase == k8s_api.PodFailed {
				break waitLoop
			}
		}
	}

	logOpts := &k8s_api.PodLogOptions{
		Container: opts.PodName,
	}
	rc := w.client.CoreV1().Pods(opts.Namespace).GetLogs(opts.PodName, logOpts)
	stream, err := rc.Stream(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to stream pod logs: %w", err)
	}
	defer stream.Close()

	stdout := new(bytes.Buffer)

	_, err = io.Copy(stdout, stream)
	if err != nil {
		return stdout, xerrors.Errorf("failed copying pod logs: %w", err)
	}

	_ = w.client.CoreV1().Pods(opts.Namespace).Delete(ctx, opts.PodName, k8s_api_meta.DeleteOptions{})
	return stdout, nil
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
