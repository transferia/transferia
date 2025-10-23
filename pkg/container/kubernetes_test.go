package container

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

func TestK8sOptsString(t *testing.T) {
	opts := K8sOpts{
		Namespace:     "default",
		PodName:       "example-pod",
		Image:         "nginx:latest",
		RestartPolicy: corev1.RestartPolicyAlways,
		Command:       []string{"nginx"},
		Args:          []string{"-g", "daemon off;"},
		Env: []corev1.EnvVar{
			{
				Name:  "ENV_VAR",
				Value: "value",
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "data",
				MountPath: "/data",
			},
		},
		Timeout: 30 * time.Second,
	}

	yamlData := opts.String()

	var pod corev1.Pod
	if err := yaml.Unmarshal([]byte(yamlData), &pod); err != nil {
		t.Fatalf("Unmarshalling pod YAML failed: %v", err)
	}

	// Verify that the pod metadata matches.
	if pod.ObjectMeta.Name != opts.PodName {
		t.Errorf("Expected pod name %q, got %q", opts.PodName, pod.ObjectMeta.Name)
	}
	if pod.ObjectMeta.Namespace != opts.Namespace {
		t.Errorf("Expected namespace %q, got %q", opts.Namespace, pod.ObjectMeta.Namespace)
	}

	// Verify restart policy.
	if pod.Spec.RestartPolicy != opts.RestartPolicy {
		t.Errorf("Expected restart policy %q, got %q", opts.RestartPolicy, pod.Spec.RestartPolicy)
	}

	// Verify that there's one container and its fields match.
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("Expected exactly one container, got %d", len(pod.Spec.Containers))
	}

	container := pod.Spec.Containers[0]
	if container.Image != opts.Image {
		t.Errorf("Expected image %q, got %q", opts.Image, container.Image)
	}
	if len(container.Command) != len(opts.Command) {
		t.Errorf("Expected %d command(s), got %d", len(opts.Command), len(container.Command))
	}
	for i, v := range container.Command {
		if v != opts.Command[i] {
			t.Errorf("Expected command at index %d to be %q, got %q", i, opts.Command[i], v)
		}
	}
	if len(container.Args) != len(opts.Args) {
		t.Errorf("Expected %d arg(s), got %d", len(opts.Args), len(container.Args))
	}
	for i, v := range container.Args {
		if v != opts.Args[i] {
			t.Errorf("Expected arg at index %d to be %q, got %q", i, opts.Args[i], v)
		}
	}

	// Verify environment variables.
	if len(container.Env) != len(opts.Env) {
		t.Errorf("Expected %d environment variables, got %d", len(opts.Env), len(container.Env))
	} else {
		for i, envVar := range container.Env {
			if envVar.Name != opts.Env[i].Name || envVar.Value != opts.Env[i].Value {
				t.Errorf("Expected env var at index %d to be %+v, got %+v", i, opts.Env[i], envVar)
			}
		}
	}

	// Verify VolumeMounts in container.
	if len(container.VolumeMounts) != len(opts.VolumeMounts) {
		t.Errorf("Expected %d volumeMount(s), got %d", len(opts.VolumeMounts), len(container.VolumeMounts))
	} else {
		for i, vm := range container.VolumeMounts {
			if vm.Name != opts.VolumeMounts[i].Name || vm.MountPath != opts.VolumeMounts[i].MountPath {
				t.Errorf("Expected volumeMount at index %d to be %+v, got %+v", i, opts.VolumeMounts[i], vm)
			}
		}
	}

	// Verify Volumes at pod spec level.
	if len(pod.Spec.Volumes) != len(opts.Volumes) {
		t.Errorf("Expected %d volume(s), got %d", len(opts.Volumes), len(pod.Spec.Volumes))
	} else {
		for i, vol := range pod.Spec.Volumes {
			if vol.Name != opts.Volumes[i].Name {
				t.Errorf("Expected volume at index %d to have name %q, got %q", i, opts.Volumes[i].Name, vol.Name)
			}
		}
	}
}

func TestK8sOptsString_MarshalError(t *testing.T) {
	opts := K8sOpts{
		Namespace:     "default",
		PodName:       "test-pod",
		Image:         "busybox:latest",
		RestartPolicy: corev1.RestartPolicyOnFailure,
		Command:       []string{"sleep"},
		Args:          []string{"3600"},
		Env:           []corev1.EnvVar{},
		Volumes:       []corev1.Volume{},
		VolumeMounts:  []corev1.VolumeMount{},
		Timeout:       10 * time.Second,
	}

	yamlStr := opts.String()
	if yamlStr == "" {
		t.Error("Expected a non-empty YAML string")
	}
	var pod corev1.Pod
	if err := yaml.Unmarshal([]byte(yamlStr), &pod); err != nil {
		t.Errorf("Expected valid YAML, but got error: %v", err)
	}
}
