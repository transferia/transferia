package container

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Secret represents a Kubernetes secret to be created
type Secret struct {
	Name string
	Data map[string][]byte
}

type Volume struct {
	Name          string
	HostPath      string
	SecretName    string // Name of the secret to mount (for Kubernetes)
	ContainerPath string
	VolumeType    string // "bind", "secret", etc.
	ReadOnly      bool
}

type ContainerOpts struct {
	Env                        map[string]string
	LogOptions                 map[string]string
	Namespace                  string
	RestartPolicy              corev1.RestartPolicy
	PodName                    string
	Image                      string
	LogDriver                  string
	Network                    string
	ContainerName              string
	Volumes                    []Volume
	Secrets                    []Secret // Kubernetes secrets to create
	Command                    []string
	Args                       []string
	Timeout                    time.Duration
	AttachStdout               bool
	AttachStderr               bool
	AutoRemove                 bool
	JobTTLSecondsAfterFinished *int32 // TTL for Kubernetes Jobs after completion (in seconds)
}

func (c *ContainerOpts) String() string {
	if isRunningInKubernetes() {
		return c.ToK8sOpts(nil).String()
	}

	return c.ToDockerOpts().String()
}
