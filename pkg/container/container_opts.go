package container

import (
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	corev1 "k8s.io/api/core/v1"
)

type Volume struct {
	Name          string
	HostPath      string
	ContainerPath string
	VolumeType    string
	ReadOnly      bool
}

type ContainerOpts struct {
	Env           map[string]string
	LogOptions    map[string]string
	Namespace     string
	RestartPolicy corev1.RestartPolicy
	PodName       string
	Image         string
	LogDriver     string
	Network       string
	ContainerName string
	Volumes       []Volume
	Command       []string
	Args          []string
	Timeout       time.Duration
	AttachStdout  bool
	AttachStderr  bool
	AutoRemove    bool
}

func (c *ContainerOpts) String() string {
	if isRunningInKubernetes() {
		return c.ToK8sOpts().String()
	}

	return c.ToDockerOpts().String()
}

func (c *ContainerOpts) ToDockerOpts() DockerOpts {
	var envSlice []string
	for key, value := range c.Env {
		envSlice = append(envSlice, key+"="+value)
	}

	var mounts []mount.Mount
	for _, vol := range c.Volumes {
		mounts = append(mounts, mount.Mount{
			Type:     mount.Type(vol.VolumeType),
			Source:   vol.HostPath,
			Target:   vol.ContainerPath,
			ReadOnly: vol.ReadOnly,
		})
	}

	var restartPolicy container.RestartPolicy
	switch c.RestartPolicy {
	case corev1.RestartPolicyAlways:
		restartPolicy.Name = container.RestartPolicyAlways
	case corev1.RestartPolicyOnFailure:
		restartPolicy.Name = container.RestartPolicyOnFailure
	case corev1.RestartPolicyNever:
		restartPolicy.Name = container.RestartPolicyDisabled
	}

	return DockerOpts{
		RestartPolicy: restartPolicy,
		Mounts:        mounts,
		LogDriver:     c.LogDriver,
		LogOptions:    c.LogOptions,
		Image:         c.Image,
		Network:       c.Network,
		ContainerName: c.ContainerName,
		Command:       c.Command,
		Env:           envSlice,
		Timeout:       c.Timeout,
		AutoRemove:    c.AutoRemove,
		AttachStdout:  c.AttachStdout,
		AttachStderr:  c.AttachStderr,
	}
}

func (c *ContainerOpts) ToK8sOpts() K8sOpts {
	var envVars []corev1.EnvVar
	for key, value := range c.Env {
		envVars = append(envVars, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	var k8sVolumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount
	for _, vol := range c.Volumes {
		volumeName := vol.Name
		k8sVolumes = append(k8sVolumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: vol.HostPath,
				},
			},
		})

		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: vol.ContainerPath,
			ReadOnly:  vol.ReadOnly,
		})
	}

	return K8sOpts{
		Namespace:     c.Namespace,
		PodName:       c.PodName,
		ContainerName: c.ContainerName,
		Image:         c.Image,
		RestartPolicy: c.RestartPolicy,
		Command:       c.Command,
		Args:          c.Args,
		Env:           envVars,
		Volumes:       k8sVolumes,
		VolumeMounts:  volumeMounts,
		Timeout:       c.Timeout,
	}
}
