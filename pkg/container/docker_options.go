package container

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	corev1 "k8s.io/api/core/v1"
)

type DockerOpts struct {
	RestartPolicy container.RestartPolicy
	Mounts        []mount.Mount
	LogDriver     string
	LogOptions    map[string]string
	Image         string
	Network       string
	ContainerName string
	Command       []string
	Env           []string
	Timeout       time.Duration
	AutoRemove    bool
	AttachStdout  bool
	AttachStderr  bool
}

func (o DockerOpts) String() string {
	var args []string

	// AutoRemove
	if o.AutoRemove {
		args = append(args, "--rm")
	}

	// ContainerName
	if o.ContainerName != "" {
		args = append(args, "--name", o.ContainerName)
	}

	// Network
	if o.Network != "" {
		args = append(args, "--network", o.Network)
	}

	// Mounts
	if len(o.Mounts) > 0 {
		sort.Slice(o.Mounts, func(i, j int) bool {
			ikey := fmt.Sprintf("%s/%s/%s", o.Mounts[i].Type, o.Mounts[i].Source, o.Mounts[i].Target)
			jkey := fmt.Sprintf("%s/%s/%s", o.Mounts[j].Type, o.Mounts[j].Source, o.Mounts[j].Target)
			return ikey < jkey
		})
		for _, m := range o.Mounts {
			mountOpts := []string{
				fmt.Sprintf("type=%s", m.Type),
				fmt.Sprintf("source=%s", m.Source),
				fmt.Sprintf("target=%s", m.Target),
			}
			if m.ReadOnly {
				mountOpts = append(mountOpts, "readonly")
			}

			args = append(args, "--mount", strings.Join(mountOpts, ","))
		}
	}

	// Environment Variables
	if len(o.Env) > 0 {
		sort.Strings(o.Env)
		for _, envVar := range o.Env {
			args = append(args, "-e", envVar)
		}
	}

	// Log Driver and options
	if o.LogDriver != "" {
		args = append(args, "--log-driver", o.LogDriver)
		if len(o.LogOptions) > 0 {
			var logOptKeys []string
			for key := range o.LogOptions {
				logOptKeys = append(logOptKeys, key)
			}
			sort.Strings(logOptKeys)
			for _, key := range logOptKeys {
				value := o.LogOptions[key]
				args = append(args, "--log-opt", fmt.Sprintf("%s=%s", key, value))
			}
		}
	}

	// Attach
	var attachOptions []string
	if o.AttachStderr {
		attachOptions = append(attachOptions, "stderr")
	}
	if o.AttachStdout {
		attachOptions = append(attachOptions, "stdout")
	}
	if len(attachOptions) > 0 {
		sort.Strings(attachOptions)
		for _, attach := range attachOptions {
			args = append(args, "--attach", attach)
		}
	}

	// Image
	if o.Image != "" {
		args = append(args, o.Image)
	}

	// Command
	if len(o.Command) > 0 {
		args = append(args, o.Command...)
	}

	// RestartPolicy
	if o.RestartPolicy.Name != "" {
		args = append(args, "--restart", string(o.RestartPolicy.Name))
	}

	cmd := append([]string{"docker", "run"}, args...)

	return strings.Join(cmd, " ")
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
