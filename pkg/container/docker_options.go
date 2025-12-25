package container

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
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
