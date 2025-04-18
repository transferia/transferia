package container

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

type K8sOpts struct {
	Namespace     string
	PodName       string
	ContainerName string
	Image         string
	RestartPolicy corev1.RestartPolicy
	Command       []string
	Args          []string
	Env           []corev1.EnvVar
	Volumes       []corev1.Volume
	VolumeMounts  []corev1.VolumeMount
	Timeout       time.Duration
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
