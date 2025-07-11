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
	"go.ytsaurus.tech/library/go/core/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

type K8sWrapper struct {
	client kubernetes.Interface
	logger log.Logger
}

func NewK8sWrapper(logger log.Logger) (*K8sWrapper, error) {
	logger.Info("Initializing Kubernetes wrapper")
	config, err := rest.InClusterConfig()
	if err != nil {
		logger.Errorf("Failed to load in-cluster config: %v", err)
		return nil, xerrors.Errorf("failed to load in-cluster config: %w", err)
	}
	logger.Info("Successfully loaded in-cluster config")

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Errorf("Failed to create Kubernetes client: %v", err)
		return nil, xerrors.Errorf("failed to create k8s client: %w", err)
	}
	logger.Info("Successfully created Kubernetes client")

	return &K8sWrapper{client: clientset, logger: logger}, nil
}

func (w *K8sWrapper) Pull(_ context.Context, _ string, _ types.ImagePullOptions) error {
	// No need to pull images in k8s
	return nil
}

func (w *K8sWrapper) getCurrentNamespace() (string, error) {
	b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		w.logger.Warnf("Failed to read namespace from service account: %v", err)
		return "", err
	}
	namespace := string(b)
	return namespace, nil
}

// Helper method to create a job
func (w *K8sWrapper) createJob(ctx context.Context, opts K8sOpts, suspend bool) (*batchv1.Job, error) {
	if opts.PodName == "" {
		opts.PodName = "transferia-runner"
		w.logger.Info("Using default job name: transferia-runner")
	}

	if opts.ContainerName == "" {
		opts.ContainerName = "runner"
		w.logger.Info("Using default container name: runner")
	}

	w.logger.Infof("Creating job %s in namespace %s with image %s (suspended: %v)",
		opts.PodName, opts.Namespace, opts.Image, suspend)

	// Set backoffLimit to 0 to prevent retries
	backoffLimit := int32(0)

	// Create a unique label selector to identify pods created by this job
	jobName := fmt.Sprintf("%s-%s", opts.PodName, time.Now().Format("20060102-150405"))
	labels := map[string]string{
		"app":        "transferia",
		"created-by": "transferia-runner",
		"job-name":   jobName,
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: opts.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: opts.JobTTLSecondsAfterFinished,
			Suspend:                 &suspend,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
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
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	createdJob, err := w.client.BatchV1().Jobs(opts.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		w.logger.Errorf("Failed to create job: %v", err)
		return nil, err
	}

	w.logger.Infof("Successfully created job %s", createdJob.Name)

	return createdJob, nil
}

// Helper method to find the pod created by a job
func (w *K8sWrapper) findJobPod(ctx context.Context, job *batchv1.Job) (*corev1.Pod, error) {
	w.logger.Infof("Finding pod for job %s", job.Name)

	labelSelector := labels.SelectorFromSet(job.Spec.Template.Labels)
	listOptions := metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	}

	// Wait for the pod to be created
	var pod *corev1.Pod
	maxRetries := 10
	for range maxRetries {
		pods, err := w.client.CoreV1().Pods(job.Namespace).List(ctx, listOptions)
		if err != nil {
			w.logger.Warnf("Error listing pods for job %s: %v", job.Name, err)
			return nil, err
		}

		if len(pods.Items) > 0 {
			pod = &pods.Items[0]
			w.logger.Infof("Found pod %s for job %s", pod.Name, job.Name)
			return pod, nil
		}

		w.logger.Infof("No pods found for job %s yet, waiting...", job.Name)
		time.Sleep(1 * time.Second)
	}

	return nil, xerrors.Errorf("no pods found for job %s after %d retries", job.Name, maxRetries)
}

func (w *K8sWrapper) ensureSecret(ctx context.Context, namespace string, secret *corev1.Secret, ownerRef *metav1.OwnerReference) error {
	w.logger.Infof("Ensuring secret %s exists in namespace %s", secret.Name, namespace)

	// Add owner reference if provided
	if ownerRef != nil {
		if secret.OwnerReferences == nil {
			secret.OwnerReferences = []metav1.OwnerReference{*ownerRef}
		} else {
			// Check if owner reference already exists
			exists := false
			for _, ref := range secret.OwnerReferences {
				if ref.UID == ownerRef.UID {
					exists = true
					break
				}
			}
			if !exists {
				secret.OwnerReferences = append(secret.OwnerReferences, *ownerRef)
			}
		}
	}

	// Check if secret already exists
	_, err := w.client.CoreV1().Secrets(namespace).Get(ctx, secret.Name, metav1.GetOptions{})
	if err != nil {
		// Secret doesn't exist, create it
		_, err = w.client.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			w.logger.Errorf("Failed to create secret %s: %v", secret.Name, err)
			return xerrors.Errorf("failed to create secret %s: %w", secret.Name, err)
		}
		w.logger.Infof("Successfully created secret %s", secret.Name)
	} else {
		// Secret exists, update it
		_, err = w.client.CoreV1().Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{})
		if err != nil {
			w.logger.Errorf("Failed to update secret %s: %v", secret.Name, err)
			return xerrors.Errorf("failed to update secret %s: %w", secret.Name, err)
		}
		w.logger.Infof("Successfully updated secret %s", secret.Name)
	}

	return nil
}

func (w *K8sWrapper) Run(ctx context.Context, opts ContainerOpts) (stdout io.ReadCloser, stderr io.ReadCloser, err error) {
	// Convert options to K8s options
	k8sOpts := opts.ToK8sOpts(w)

	// Store secrets for later creation with owner reference
	var secretsToCreate []struct {
		secret *corev1.Secret
		name   string
	}

	if len(k8sOpts.Secrets) > 0 {
		for _, secret := range k8sOpts.Secrets {
			k8sSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secret.Name,
					Namespace: k8sOpts.Namespace,
				},
				Data: secret.Data,
				Type: corev1.SecretTypeOpaque,
			}

			secretsToCreate = append(secretsToCreate, struct {
				secret *corev1.Secret
				name   string
			}{
				secret: k8sSecret,
				name:   secret.Name,
			})
		}
	}

	job, err := w.createJob(ctx, k8sOpts, true)
	if err != nil {
		w.logger.Errorf("Failed to create suspended job %s: %v", k8sOpts.PodName, err)
		return nil, nil, xerrors.Errorf("failed to create suspended job: %w", err)
	}

	if len(secretsToCreate) > 0 {
		// Create owner reference
		ownerRef := &metav1.OwnerReference{
			APIVersion: "batch/v1",
			Kind:       "Job",
			Name:       job.Name,
			UID:        job.UID,
			Controller: &[]bool{true}[0],
		}

		for _, s := range secretsToCreate {
			if err := w.ensureSecret(ctx, k8sOpts.Namespace, s.secret, ownerRef); err != nil {
				w.logger.Errorf("Failed to ensure secret %s: %v", s.name, err)
				return nil, nil, err
			}
		}
	}

	w.logger.Infof("Unsuspending job %s to allow execution", job.Name)

	var updatedJob *batchv1.Job
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get the latest version of the job in case it was modified
		latestJob, err := w.client.BatchV1().Jobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		// Apply our change to the latest version
		suspend := false
		latestJob.Spec.Suspend = &suspend

		// Try to update with the latest version
		updatedJob, err = w.client.BatchV1().Jobs(latestJob.Namespace).Update(ctx, latestJob, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		w.logger.Errorf("Failed to unsuspend job %s after retries: %v", job.Name, err)
		return nil, nil, xerrors.Errorf("failed to unsuspend job after retries: %w", err)
	}
	w.logger.Infof("Successfully unsuspended job %s", updatedJob.Name)

	// Use the updated job for further operations
	job = updatedJob

	// Find the pod created by the job
	pod, err := w.findJobPod(ctx, job)
	if err != nil {
		w.logger.Errorf("Failed to find pod for job %s: %v", job.Name, err)
		return nil, nil, xerrors.Errorf("failed to find pod for job: %w", err)
	}

	logStreamingDone := make(chan struct{})

	// Wait for pod to reach Running / Completed / Failed state before trying to stream logs
	w.logger.Infof("Waiting for pod %s to be ready...", pod.Name)
	if err := w.waitForPodReady(ctx, pod.GetNamespace(), pod.GetName(), 30*time.Minute); err != nil {
		// If pod can't get to running state, delete the job and return the error
		w.logger.Errorf("Pod %s failed to become ready after 30 minutes: %v", pod.GetName(), err)

		// Delete the job
		deletePolicy := metav1.DeletePropagationForeground
		deleteOptions := metav1.DeleteOptions{
			PropagationPolicy: &deletePolicy,
		}

		if deleteErr := w.client.BatchV1().Jobs(job.Namespace).Delete(ctx, job.Name, deleteOptions); deleteErr != nil {
			w.logger.Errorf("Failed to delete job %s: %v", job.Name, deleteErr)
			// Continue with the original error even if deletion fails
		} else {
			w.logger.Infof("Successfully deleted job %s", job.Name)
		}

		return nil, nil, xerrors.Errorf("pod failed to become ready: %w", err)
	}

	// Set up log streaming options
	logOpts := &corev1.PodLogOptions{
		Container: k8sOpts.ContainerName,
		Follow:    true,
	}

	// Get logs stream
	w.logger.Infof("Setting up log streaming for pod %s", pod.Name)
	req := w.client.CoreV1().Pods(pod.GetNamespace()).GetLogs(pod.GetName(), logOpts)
	stream, err := req.Stream(ctx)
	if err != nil {
		// If we can't get logs, close the logStreamingDone channel
		close(logStreamingDone)
		return nil, nil, xerrors.Errorf("failed to stream pod logs: %w", err)
	}
	w.logger.Infof("Successfully established log stream for pod %s", pod.Name)

	// Wrap the stream to signal when it's closed
	wrappedStream := &streamWrapper{
		ReadCloser: stream,
		onClose: func() {
			close(logStreamingDone)
		},
	}

	// Return the wrapped stream
	return wrappedStream, nil, nil
}

// Helper to wait for pod to be ready
func (w *K8sWrapper) waitForPodReady(ctx context.Context, namespace, name string, timeout time.Duration) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			return xerrors.New("timeout waiting for pod to be ready")

		case <-time.After(1 * time.Second):
			pod, err := w.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					continue // Pod might not be created yet
				}
				return xerrors.Errorf("error getting pod: %w", err)
			}

			w.logger.Infof("Pod %s status: %s", pod.Name, pod.Status.Phase)
			// If pod is running or succeeded/failed, it's ready for logs
			if pod.Status.Phase == corev1.PodRunning ||
				pod.Status.Phase == corev1.PodSucceeded ||
				pod.Status.Phase == corev1.PodFailed {
				// Ready to collect logs
				return nil
			}

			// If pod is in error state, return an error
			if pod.Status.Phase == corev1.PodUnknown {
				return xerrors.New("pod is in Unknown phase")
			}
		}
	}
}

// RunAndWait creates a job and waits for it to complete, collecting logs
func (w *K8sWrapper) RunAndWait(ctx context.Context, opts ContainerOpts) (*bytes.Buffer, *bytes.Buffer, error) {
	stdoutReader, _, err := w.Run(ctx, opts)
	if err != nil {
		w.logger.Errorf("Failed to run job: %v", err)
		return nil, nil, err
	}
	defer stdoutReader.Close()

	// Convert options to K8s options
	k8sOpts := opts.ToK8sOpts(w)
	// Note: We can't predict the job name here as it's generated in createJob
	// So we need to use labels to find the job
	jobLabels := map[string]string{
		"app":        "transferia",
		"created-by": "transferia-runner",
	}
	labelSelector := labels.SelectorFromSet(jobLabels)
	listOptions := metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	}

	// Find the most recent job
	jobs, err := w.client.BatchV1().Jobs(k8sOpts.Namespace).List(ctx, listOptions)
	if err != nil || len(jobs.Items) == 0 {
		return nil, nil, xerrors.Errorf("failed to find job: %w", err)
	}

	// Use the most recent job (should be the one we just created)
	jobName := jobs.Items[0].Name
	for _, job := range jobs.Items {
		if job.CreationTimestamp.After(jobs.Items[0].CreationTimestamp.Time) {
			jobName = job.Name
		}
	}

	// Start a goroutine to watch the job status
	jobDone := make(chan error)
	go func() {
		w.logger.Infof("Watching job %s for completion", jobName)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				jobDone <- ctx.Err()
				return
			case <-ticker.C:
				job, err := w.client.BatchV1().Jobs(k8sOpts.Namespace).Get(ctx, jobName, metav1.GetOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						w.logger.Warnf("Job %s not found, assuming it was deleted", jobName)
						jobDone <- nil
						return
					}
					jobDone <- xerrors.New(fmt.Sprintf("error getting job %s status: %v", jobName, err))
					return
				}

				// Check if job is complete
				if job.Status.Succeeded > 0 {
					jobDone <- nil
					return
				} else if job.Status.Failed > 0 {
					jobDone <- xerrors.New("job failed")
					return
				}
			}
		}
	}()

	stdoutBuf := new(bytes.Buffer)

	// Copy logs to buffer
	bytesRead, err := io.Copy(stdoutBuf, stdoutReader)
	if err != nil && err != io.EOF {
		w.logger.Errorf("Error copying pod logs: %v", err)
		return stdoutBuf, nil, xerrors.Errorf("error copying pod logs: %w", err)
	}

	// Wait for job to complete
	select {
	case err := <-jobDone:
		if err != nil {
			w.logger.Errorf("Job monitoring error: %v", err)
			return stdoutBuf, nil, xerrors.Errorf("job monitoring error: %w", err)
		}
	case <-time.After(k8sOpts.Timeout):
		w.logger.Warn("Timeout waiting for job to complete")
		return stdoutBuf, nil, xerrors.New("timeout waiting for job to complete")
	}

	w.logger.Infof("Job execution completed, collected %d bytes of logs", bytesRead)
	return stdoutBuf, nil, nil
}

// Type returns the container backend type
func (w *K8sWrapper) Type() ContainerBackend {
	return BackendKubernetes
}
