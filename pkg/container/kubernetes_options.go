package container

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

type K8sOpts struct {
	Namespace                  string
	PodName                    string
	ContainerName              string
	Image                      string
	RestartPolicy              corev1.RestartPolicy
	Command                    []string
	Args                       []string
	Env                        []corev1.EnvVar
	Volumes                    []corev1.Volume
	VolumeMounts               []corev1.VolumeMount
	Secrets                    []Secret // Kubernetes secrets to create
	Timeout                    time.Duration
	JobTTLSecondsAfterFinished *int32 // TTL for Kubernetes Jobs after completion (in seconds)
}

func (k K8sOpts) String() string {
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.PodName,
			Namespace: k.Namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: k.RestartPolicy,
			Containers: []corev1.Container{
				{
					Name:         k.PodName,
					Image:        k.Image,
					Command:      k.Command,
					Args:         k.Args,
					Env:          k.Env,
					VolumeMounts: k.VolumeMounts,
				},
			},
			Volumes: k.Volumes,
		},
	}

	b, err := yaml.Marshal(pod)
	if err != nil {
		return fmt.Sprintf("error marshalling pod to YAML: %v", err)
	}

	return string(b)
}

func (c *ContainerOpts) ToK8sOpts(w *K8sWrapper) K8sOpts {
	var envVars []corev1.EnvVar
	for key, value := range c.Env {
		envVars = append(envVars, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	var k8sVolumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	// Process regular volumes and secret volumes
	for _, vol := range c.Volumes {
		volumeName := vol.Name

		// Create the appropriate volume source based on volume type
		if vol.VolumeType == "secret" && vol.SecretName != "" {
			// Secret volume
			k8sVolumes = append(k8sVolumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: vol.SecretName,
					},
				},
			})
		} else {
			// Default to host path volume
			k8sVolumes = append(k8sVolumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: vol.HostPath,
					},
				},
			})
		}

		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: vol.ContainerPath,
			ReadOnly:  vol.ReadOnly,
		})
	}

	// Set default TTL for Jobs if not specified
	var jobTTL *int32
	if c.JobTTLSecondsAfterFinished != nil {
		jobTTL = c.JobTTLSecondsAfterFinished
	} else {
		defaultTTL := int32(3600) // Default to 1 hour
		jobTTL = &defaultTTL
	}

	namespace := c.Namespace
	if namespace == "" && w == nil {
		namespace = "default"
	}

	if namespace == "" && w != nil {
		ns, err := w.getCurrentNamespace()
		if err != nil {
			ns = "default"
		}
		namespace = ns
	}

	return K8sOpts{
		Namespace:                  namespace,
		PodName:                    c.PodName,
		ContainerName:              c.ContainerName,
		Image:                      c.Image,
		RestartPolicy:              c.RestartPolicy,
		Command:                    c.Command,
		Args:                       c.Args,
		Env:                        envVars,
		Secrets:                    c.Secrets,
		Volumes:                    k8sVolumes,
		VolumeMounts:               volumeMounts,
		Timeout:                    c.Timeout,
		JobTTLSecondsAfterFinished: jobTTL,
	}
}
