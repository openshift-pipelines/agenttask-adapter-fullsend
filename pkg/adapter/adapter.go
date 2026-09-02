/*
Copyright 2026 The AgentTask Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package adapter contains the experimental Fullsend AgentTask Adapter.
package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	"unicode/utf8"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	"github.com/openshift-pipelines/agenttask/pkg/framework"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	Selector = "fullsend.ai/agenttask-adapter"

	ProfileFixture = "fixture-v1"
	ParamProfile   = "profile"
	ParamRequest   = "request"

	ResultOutcome      = "outcome"
	ResultOutputPVC    = "output-pvc"
	ResultOutputPath   = "output-path"
	ResultOutputDigest = "output-digest"

	OutcomeCompleted = "completed"
	OutcomeSkipped   = "skipped"

	OutputPVCName        = "agenttask-output"
	RunnerServiceAccount = "fullsend-job"
	ContainerName        = "fullsend"

	attemptAnnotation = "agent.tekton.dev/attempt-id"
	profileAnnotation = "agent.tekton.dev/profile-digest"
	attemptLabel      = "agent.tekton.dev/attempt"
	customRunUIDLabel = "agent.tekton.dev/customrun-uid"
)

const (
	pollInterval    = 2 * time.Second
	maxRequestBytes = 32768
	outputMountPath = "/workspace/output"

	// ponytail: fixed PoC bounds; make these installation-configurable before production.
	createSettleTime = 5 * time.Second
	cleanupTimeout   = 2 * time.Minute
)

// Adapter maps one AgentTask CustomRun to a deterministic Kubernetes Job.
type Adapter struct {
	Client client.Client
	Image  string
	Now    func() time.Time
}

var _ framework.AgentTaskAdapter = (*Adapter)(nil)

func (*Adapter) Name(context.Context) string { return Selector }

func (a *Adapter) Validate(_ context.Context, task *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun) error {
	if a.Client == nil {
		return fmt.Errorf("kubernetes client is required")
	}
	if a.Image == "" {
		return fmt.Errorf("fullsend image is required")
	}
	if task == nil || run == nil {
		return fmt.Errorf("agent task and custom run are required")
	}
	if task.Namespace != run.Namespace {
		return fmt.Errorf("agent task and custom run must share a namespace")
	}
	if len(task.Spec.Workspaces) != 0 || len(run.Spec.Workspaces) != 0 {
		return fmt.Errorf("%w: profile %s", framework.ErrWorkspaceNotSupported, ProfileFixture)
	}
	if run.Spec.ServiceAccountName != "" && run.Spec.ServiceAccountName != "default" {
		return fmt.Errorf("profile %s uses the fixed %s service account", ProfileFixture, RunnerServiceAccount)
	}
	if err := validateProfile(task); err != nil {
		return err
	}
	if err := validateParams(task); err != nil {
		return err
	}
	if err := validateResults(task); err != nil {
		return err
	}
	_, err := requestValue(run)
	return err
}

func (a *Adapter) Reconcile(ctx context.Context, request framework.Request) (framework.Observation, error) {
	name := executionName(request.AttemptID)
	var job batchv1.Job
	err := a.Client.Get(ctx, client.ObjectKey{Namespace: request.CustomRun.Namespace, Name: name}, &job)
	if apierrors.IsNotFound(err) {
		if request.ExecutionRef != nil {
			return failedObservation(request.ExecutionRef, framework.ReasonInfrastructureFailed, "The Fullsend Job disappeared before completion"), nil
		}
		desired, buildErr := a.desiredJob(request, name)
		if buildErr != nil {
			return framework.Observation{}, buildErr
		}
		if createErr := a.Client.Create(ctx, desired); createErr != nil {
			if getErr := a.Client.Get(ctx, client.ObjectKeyFromObject(desired), &job); getErr != nil {
				if apierrors.IsNotFound(getErr) {
					return framework.Observation{}, createErr
				}
				return framework.Observation{}, getErr
			}
		} else {
			job = *desired
		}
	} else if err != nil {
		return framework.Observation{}, err
	}

	if err := a.validateOwnedJob(&job, request); err != nil {
		return failedObservation(request.ExecutionRef, framework.ReasonInfrastructureFailed, "Existing Fullsend Job failed ownership validation"), nil
	}
	reference, err := executionReference(&job)
	if err != nil {
		return framework.Observation{}, err
	}
	return a.observe(ctx, &job, reference)
}

func (a *Adapter) Cancel(ctx context.Context, request framework.Request) (framework.Observation, error) {
	name := executionName(request.AttemptID)
	var job batchv1.Job
	if err := a.Client.Get(ctx, client.ObjectKey{Namespace: request.CustomRun.Namespace, Name: name}, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			if a.cleanupExpired(request.CustomRun) {
				return cleanupFailed(request.ExecutionRef, "Fullsend Job cleanup could not be observed"), nil
			}
			return framework.Observation{}, err
		}
		if reference := request.ExecutionRef; reference != nil {
			if reference.APIVersion != batchv1.SchemeGroupVersion.String() || reference.Kind != "Job" || reference.Namespace != request.CustomRun.Namespace ||
				reference.Name != name || reference.UID == "" {
				return framework.Observation{}, fmt.Errorf("persisted Fullsend Job identity is invalid")
			}
		}
		remaining, listErr := a.ownedPodsRemain(ctx, request)
		if listErr != nil {
			if a.cleanupExpired(request.CustomRun) {
				return cleanupFailed(request.ExecutionRef, "Fullsend Pod cleanup could not be observed"), nil
			}
			return framework.Observation{}, listErr
		}
		if remaining {
			if a.cleanupExpired(request.CustomRun) {
				return cleanupFailed(request.ExecutionRef, "Fullsend Pod cleanup could not be confirmed"), nil
			}
			return framework.Observation{State: framework.StateCancelling, Message: "Waiting for Fullsend Pod deletion", RequeueAfter: pollInterval}, nil
		}
		if request.ExecutionRef == nil && !a.absenceSettled(request.CustomRun) {
			return framework.Observation{State: framework.StateCancelling, Message: "Confirming no Fullsend Job was created", RequeueAfter: pollInterval}, nil
		}
		return framework.Observation{State: framework.StateCancelled, CleanupComplete: true}, nil
	}
	if err := a.validateOwnedJob(&job, request); err != nil {
		return cleanupFailed(request.ExecutionRef, "Fullsend Job ownership validation failed"), nil
	}
	reference, err := executionReference(&job)
	if err != nil {
		return framework.Observation{}, err
	}
	if job.DeletionTimestamp.IsZero() {
		uid := job.UID
		foreground := metav1.DeletePropagationForeground
		if err := a.Client.Delete(ctx, &job, &client.DeleteOptions{
			Preconditions:     &metav1.Preconditions{UID: &uid},
			PropagationPolicy: &foreground,
		}); err != nil && !apierrors.IsNotFound(err) {
			if a.cleanupExpired(request.CustomRun) {
				return cleanupFailed(reference, "Fullsend Job cleanup request did not complete"), nil
			}
			return framework.Observation{}, err
		}
	}
	if a.cleanupExpired(request.CustomRun) {
		return cleanupFailed(reference, "Fullsend Job cleanup could not be confirmed"), nil
	}
	return framework.Observation{
		State: framework.StateCancelling, Message: "Waiting for Fullsend Job deletion",
		ExecutionRef: reference, RequeueAfter: pollInterval,
	}, nil
}

func (a *Adapter) observe(ctx context.Context, job *batchv1.Job, reference *framework.ExecutionReference) (framework.Observation, error) {
	base := framework.Observation{ExecutionRef: reference, RequeueAfter: pollInterval}
	if !job.DeletionTimestamp.IsZero() {
		return failedObservation(reference, framework.ReasonInfrastructureFailed, "The Fullsend Job is being deleted unexpectedly"), nil
	}

	complete := jobConditionTrue(job, batchv1.JobComplete)
	failed := jobConditionTrue(job, batchv1.JobFailed)
	if !complete && !failed {
		if job.Status.StartTime == nil && job.Status.Active == 0 {
			base.State = framework.StateAccepted
			base.Message = "Fullsend Job accepted"
			return base, nil
		}
		base.State = framework.StateRunning
		base.Message = "Fullsend Job is running"
		return base, nil
	}

	record, pod, err := a.terminationRecord(ctx, job)
	if err != nil {
		return failedObservation(reference, framework.ReasonInfrastructureFailed, err.Error()), nil
	}
	if complete {
		if record.ExitCode != 0 {
			return failedObservation(reference, framework.ReasonInfrastructureFailed, "Completed Fullsend Job reported a nonzero exit code"), nil
		}
		return successfulObservation(job.Namespace, pod.Name, reference, record), nil
	}
	if record.ExitCode == 0 {
		return failedObservation(reference, framework.ReasonInfrastructureFailed, "Failed Fullsend Job reported a zero exit code"), nil
	}
	return failedObservation(reference, framework.ReasonAgentFailed, fmt.Sprintf("Fullsend exited with code %d", record.ExitCode)), nil
}

func (a *Adapter) desiredJob(request framework.Request, name string) (*batchv1.Job, error) {
	value, err := requestValue(request.CustomRun)
	if err != nil {
		return nil, err
	}
	outputPath := outputPath(name)
	profileDigest, err := a.profileDigest(request, outputPath)
	if err != nil {
		return nil, err
	}
	labels := map[string]string{
		attemptLabel:      correlationLabel(request.AttemptID),
		customRunUIDLabel: correlationLabel(string(request.CustomRun.UID)),
	}
	job := &batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: request.CustomRun.Namespace, Labels: labels,
			Annotations: map[string]string{attemptAnnotation: request.AttemptID, profileAnnotation: profileDigest},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(request.CustomRun, schema.GroupVersionKind{
				Group: pipelinev1beta1.SchemeGroupVersion.Group, Version: pipelinev1beta1.SchemeGroupVersion.Version, Kind: "CustomRun",
			})},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To[int32](0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{attemptAnnotation: request.AttemptID}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: ptr.To(false),
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           RunnerServiceAccount,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name: ContainerName, Image: a.Image, ImagePullPolicy: corev1.PullIfNotPresent,
						Env: []corev1.EnvVar{
							{Name: "AGENTTASK_ATTEMPT_ID", Value: request.AttemptID},
							{Name: "AGENTTASK_REQUEST", Value: value},
							{Name: "FULLSEND_OUTPUT_PVC", Value: OutputPVCName},
							{Name: "FULLSEND_OUTPUT_PATH", Value: outputPath},
							{Name: "FULLSEND_OUTPUT_ROOT", Value: outputMountPath},
						},
						TerminationMessagePath:   "/dev/termination-log",
						TerminationMessagePolicy: corev1.TerminationMessageReadFile,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							RunAsNonRoot:             ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "output", MountPath: outputMountPath}},
					}},
					Volumes: []corev1.Volume{{Name: "output", VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: OutputPVCName},
					}}},
				},
			},
		},
	}
	return job, nil
}

func (a *Adapter) validateOwnedJob(job *batchv1.Job, request framework.Request) error {
	if job.Annotations[attemptAnnotation] != request.AttemptID {
		return fmt.Errorf("existing Fullsend Job has a different attempt identity")
	}
	owner := metav1.GetControllerOf(job)
	if owner == nil || owner.UID != request.CustomRun.UID || owner.Kind != "CustomRun" {
		return fmt.Errorf("existing Fullsend Job is not controlled by the CustomRun")
	}
	bound := request.ExecutionRef != nil
	if bound {
		reference := request.ExecutionRef
		if reference.APIVersion != batchv1.SchemeGroupVersion.String() || reference.Kind != "Job" ||
			reference.Namespace != job.Namespace || reference.Name != job.Name || reference.UID != job.UID {
			return fmt.Errorf("persisted Fullsend Job identity does not match the native object")
		}
	}
	if !bound && request.AgentTask != nil {
		digest, err := a.profileDigest(request, outputPath(job.Name))
		if err != nil {
			return err
		}
		if job.Annotations[profileAnnotation] != digest {
			return fmt.Errorf("existing Fullsend Job does not match the trusted profile")
		}
	} else if job.Annotations[profileAnnotation] == "" {
		return fmt.Errorf("existing Fullsend Job has no trusted profile identity")
	}
	if err := a.validateJobShape(job, request, bound); err != nil {
		return err
	}
	return nil
}

func (a *Adapter) validateJobShape(job *batchv1.Job, request framework.Request, bound bool) error {
	expectedAttemptLabel := correlationLabel(request.AttemptID)
	expectedCustomRunLabel := correlationLabel(string(request.CustomRun.UID))
	if job.Labels[attemptLabel] != expectedAttemptLabel || job.Labels[customRunUIDLabel] != expectedCustomRunLabel ||
		job.Spec.Template.Labels[attemptLabel] != expectedAttemptLabel || job.Spec.Template.Labels[customRunUIDLabel] != expectedCustomRunLabel ||
		job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || len(job.Spec.Template.Spec.Containers) != 1 ||
		(job.Spec.Parallelism != nil && *job.Spec.Parallelism != 1) || (job.Spec.Completions != nil && *job.Spec.Completions != 1) ||
		(job.Spec.Suspend != nil && *job.Spec.Suspend) || job.Spec.ManagedBy != nil {
		return fmt.Errorf("existing Fullsend Job has an unexpected execution shape")
	}
	expectedRequest := ""
	if request.AgentTask != nil {
		var err error
		expectedRequest, err = requestValue(request.CustomRun)
		if err != nil {
			return err
		}
	}
	expectedEnv := map[string]string{
		"AGENTTASK_ATTEMPT_ID": request.AttemptID,
		"AGENTTASK_REQUEST":    expectedRequest,
		"FULLSEND_OUTPUT_PVC":  OutputPVCName,
		"FULLSEND_OUTPUT_PATH": outputPath(job.Name),
		"FULLSEND_OUTPUT_ROOT": outputMountPath,
	}
	expectedImage := a.Image
	if bound {
		expectedImage = job.Spec.Template.Spec.Containers[0].Image
	}
	if err := a.validatePodBoundary(&job.Spec.Template.Spec, expectedImage, expectedEnv, request.AgentTask == nil, false); err != nil {
		return fmt.Errorf("existing Fullsend Job has an unexpected execution shape: %w", err)
	}
	return nil
}

func (a *Adapter) validatePodBoundary(podSpec *corev1.PodSpec, expectedImage string, expectedEnv map[string]string, acceptRecordedRequest, allowImagePullSecrets bool) error {
	if podSpec == nil || len(podSpec.Containers) != 1 || len(podSpec.InitContainers) != 0 || len(podSpec.EphemeralContainers) != 0 ||
		(!allowImagePullSecrets && len(podSpec.ImagePullSecrets) != 0) || podSpec.HostNetwork || podSpec.HostPID || podSpec.HostIPC ||
		(podSpec.ShareProcessNamespace != nil && *podSpec.ShareProcessNamespace) || podSpec.ServiceAccountName != RunnerServiceAccount ||
		podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken || podSpec.RestartPolicy != corev1.RestartPolicyNever ||
		podSpec.SecurityContext == nil || podSpec.SecurityContext.RunAsNonRoot == nil || !*podSpec.SecurityContext.RunAsNonRoot ||
		podSpec.SecurityContext.SeccompProfile == nil || podSpec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault ||
		len(podSpec.SecurityContext.Sysctls) != 0 || (podSpec.SecurityContext.AppArmorProfile != nil &&
		podSpec.SecurityContext.AppArmorProfile.Type != corev1.AppArmorProfileTypeRuntimeDefault) {
		return fmt.Errorf("unexpected Pod boundary")
	}
	container := podSpec.Containers[0]
	security := container.SecurityContext
	if container.Name != ContainerName || container.Image == "" || container.Image != expectedImage || len(container.Command) != 0 || len(container.Args) != 0 ||
		container.WorkingDir != "" || len(container.EnvFrom) != 0 || len(container.Ports) != 0 || len(container.VolumeDevices) != 0 ||
		container.Stdin || container.StdinOnce || container.TTY || container.Lifecycle != nil || container.LivenessProbe != nil ||
		container.ReadinessProbe != nil || container.StartupProbe != nil || container.TerminationMessagePath != "/dev/termination-log" ||
		container.TerminationMessagePolicy != corev1.TerminationMessageReadFile || security == nil ||
		security.Privileged != nil && *security.Privileged || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation ||
		security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem || security.RunAsNonRoot == nil || !*security.RunAsNonRoot ||
		security.Capabilities == nil || len(security.Capabilities.Add) != 0 || !reflect.DeepEqual(security.Capabilities.Drop, []corev1.Capability{"ALL"}) ||
		(security.SeccompProfile != nil && security.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault) ||
		(security.ProcMount != nil && *security.ProcMount != corev1.DefaultProcMount) ||
		(security.AppArmorProfile != nil && security.AppArmorProfile.Type != corev1.AppArmorProfileTypeRuntimeDefault) {
		return fmt.Errorf("unexpected container boundary")
	}
	actualEnv := make(map[string]string, len(container.Env))
	for _, item := range container.Env {
		if item.ValueFrom != nil {
			return fmt.Errorf("environment value sources are not allowed")
		}
		if _, duplicate := actualEnv[item.Name]; duplicate {
			return fmt.Errorf("duplicate environment variable")
		}
		actualEnv[item.Name] = item.Value
	}
	if len(actualEnv) != 5 || actualEnv["AGENTTASK_ATTEMPT_ID"] != expectedEnv["AGENTTASK_ATTEMPT_ID"] ||
		actualEnv["FULLSEND_OUTPUT_PVC"] != OutputPVCName || actualEnv["FULLSEND_OUTPUT_PATH"] != expectedEnv["FULLSEND_OUTPUT_PATH"] ||
		actualEnv["FULLSEND_OUTPUT_ROOT"] != outputMountPath || !utf8.ValidString(actualEnv["AGENTTASK_REQUEST"]) ||
		actualEnv["AGENTTASK_REQUEST"] == "" || len(actualEnv["AGENTTASK_REQUEST"]) > maxRequestBytes ||
		(!acceptRecordedRequest && actualEnv["AGENTTASK_REQUEST"] != expectedEnv["AGENTTASK_REQUEST"]) {
		return fmt.Errorf("unexpected environment")
	}
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].Name != "output" || container.VolumeMounts[0].MountPath != outputMountPath ||
		container.VolumeMounts[0].ReadOnly || container.VolumeMounts[0].SubPath != "" || container.VolumeMounts[0].SubPathExpr != "" ||
		len(podSpec.Volumes) != 1 || podSpec.Volumes[0].Name != "output" || podSpec.Volumes[0].PersistentVolumeClaim == nil ||
		podSpec.Volumes[0].PersistentVolumeClaim.ClaimName != OutputPVCName || podSpec.Volumes[0].PersistentVolumeClaim.ReadOnly {
		return fmt.Errorf("unexpected output volume")
	}
	return nil
}

func (a *Adapter) profileDigest(request framework.Request, outputPath string) (string, error) {
	value, err := requestValue(request.CustomRun)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(struct {
		Image      string `json:"image"`
		AttemptID  string `json:"attemptID"`
		Request    string `json:"request"`
		OutputPVC  string `json:"outputPVC"`
		OutputPath string `json:"outputPath"`
	}{
		Image: a.Image, AttemptID: request.AttemptID, Request: value,
		OutputPVC: OutputPVCName, OutputPath: outputPath,
	})
	if err != nil {
		return "", fmt.Errorf("serialize trusted Fullsend profile: %w", err)
	}
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func (a *Adapter) terminationRecord(ctx context.Context, job *batchv1.Job) (ResultRecord, *corev1.Pod, error) {
	var pods corev1.PodList
	if err := a.Client.List(ctx, &pods, client.InNamespace(job.Namespace), client.MatchingLabels{
		batchv1.ControllerUidLabel: string(job.UID),
	}); err != nil {
		return ResultRecord{}, nil, fmt.Errorf("list Fullsend Job pods: %w", err)
	}
	owned := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		owner := metav1.GetControllerOf(&pods.Items[i])
		if owner != nil && owner.UID == job.UID && owner.Kind == "Job" {
			owned = append(owned, &pods.Items[i])
		}
	}
	if len(owned) != 1 {
		return ResultRecord{}, nil, fmt.Errorf("terminal Fullsend Job has %d owned Pods", len(owned))
	}
	pod := owned[0]
	if pod.Labels[attemptLabel] != job.Labels[attemptLabel] || pod.Labels[customRunUIDLabel] != job.Labels[customRunUIDLabel] {
		return ResultRecord{}, nil, fmt.Errorf("fullsend Pod has unexpected correlation labels")
	}
	expectedEnv := make(map[string]string, len(job.Spec.Template.Spec.Containers[0].Env))
	for _, item := range job.Spec.Template.Spec.Containers[0].Env {
		expectedEnv[item.Name] = item.Value
	}
	if err := a.validatePodBoundary(&pod.Spec, job.Spec.Template.Spec.Containers[0].Image, expectedEnv, false, true); err != nil {
		return ResultRecord{}, nil, fmt.Errorf("fullsend Pod failed execution boundary validation")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != ContainerName {
			continue
		}
		terminated := status.State.Terminated
		if terminated == nil {
			return ResultRecord{}, nil, fmt.Errorf("fullsend container has no terminated state")
		}
		record, err := ParseResultRecord([]byte(terminated.Message))
		if err != nil {
			return ResultRecord{}, nil, fmt.Errorf("invalid Fullsend termination record")
		}
		if int32(record.ExitCode) != terminated.ExitCode {
			return ResultRecord{}, nil, fmt.Errorf("fullsend termination record exit code does not match the container")
		}
		if record.Output.PVC != OutputPVCName || record.Output.Path != outputPath(job.Name) {
			return ResultRecord{}, nil, fmt.Errorf("fullsend termination record references unexpected output")
		}
		return record, pod, nil
	}
	return ResultRecord{}, nil, fmt.Errorf("fullsend Pod has no container status")
}

func (a *Adapter) ownedPodsRemain(ctx context.Context, request framework.Request) (bool, error) {
	var pods corev1.PodList
	if err := a.Client.List(ctx, &pods, client.InNamespace(request.CustomRun.Namespace), client.MatchingLabels{
		attemptLabel: correlationLabel(request.AttemptID),
	}); err != nil {
		return false, err
	}
	for i := range pods.Items {
		owner := metav1.GetControllerOf(&pods.Items[i])
		if owner == nil || owner.Kind != "Job" {
			continue
		}
		if request.ExecutionRef == nil || owner.UID == request.ExecutionRef.UID {
			return true, nil
		}
	}
	return false, nil
}

func successfulObservation(namespace, podName string, reference *framework.ExecutionReference, record ResultRecord) framework.Observation {
	outcome := OutcomeCompleted
	if record.Skipped {
		outcome = OutcomeSkipped
	}
	return framework.Observation{
		State: framework.StateSucceeded, Message: "Fullsend Job completed", ExecutionRef: reference,
		Results: []pipelinev1beta1.CustomRunResult{
			{Name: ResultOutcome, Value: outcome},
			{Name: ResultOutputPVC, Value: record.Output.PVC},
			{Name: ResultOutputPath, Value: record.Output.Path},
			{Name: ResultOutputDigest, Value: record.Output.Digest},
		},
		Logs: []framework.Reference{{
			Name: "fullsend", URI: fmt.Sprintf("k8s://v1/namespaces/%s/pods/%s", namespace, podName),
		}},
		Artifacts: []framework.Reference{{
			Name: "fullsend-output", URI: fmt.Sprintf("k8s://v1/namespaces/%s/persistentvolumeclaims/%s", namespace, record.Output.PVC),
			MediaType: "application/vnd.fullsend.run.v1", Digest: record.Output.Digest,
		}},
	}
}

func failedObservation(reference *framework.ExecutionReference, reason, message string) framework.Observation {
	return framework.Observation{State: framework.StateFailed, Reason: reason, Message: message, ExecutionRef: reference}
}

func cleanupFailed(reference *framework.ExecutionReference, message string) framework.Observation {
	return framework.Observation{
		State: framework.StateFailed, Reason: framework.ReasonCleanupFailed, Message: message,
		ExecutionRef: reference,
	}
}

func (a *Adapter) absenceSettled(run *pipelinev1beta1.CustomRun) bool {
	started := cancellationStarted(run)
	return !started.IsZero() && !a.now().Before(started.Add(createSettleTime))
}

func (a *Adapter) cleanupExpired(run *pipelinev1beta1.CustomRun) bool {
	started := cancellationStarted(run)
	return !started.IsZero() && !a.now().Before(started.Add(cleanupTimeout))
}

func cancellationStarted(run *pipelinev1beta1.CustomRun) time.Time {
	if run == nil {
		return time.Time{}
	}
	if !run.DeletionTimestamp.IsZero() {
		return run.DeletionTimestamp.Time
	}
	condition := run.Status.GetCondition(apis.ConditionSucceeded)
	if condition != nil && (condition.Reason == "Cancelling" || condition.Reason == "TimingOut") {
		return condition.LastTransitionTime.Inner.Time
	}
	return time.Time{}
}

func (a *Adapter) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func jobConditionTrue(job *batchv1.Job, conditionType batchv1.JobConditionType) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == conditionType && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func executionReference(job *batchv1.Job) (*framework.ExecutionReference, error) {
	if job.UID == "" {
		return nil, fmt.Errorf("fullsend job has no server assigned UID")
	}
	return &framework.ExecutionReference{
		APIVersion: batchv1.SchemeGroupVersion.String(), Kind: "Job",
		Namespace: job.Namespace, Name: job.Name, UID: job.UID,
	}, nil
}

func validateProfile(task *agentv1alpha1.AgentTask) error {
	if len(task.Spec.AdapterRef.Params) != 1 {
		return fmt.Errorf("adapterRef must contain only the %s parameter", ParamProfile)
	}
	profile := task.Spec.AdapterRef.Params[0]
	if profile.Name != ParamProfile || profile.Value.Type != pipelinev1.ParamTypeString || profile.Value.StringVal != ProfileFixture {
		return fmt.Errorf("adapterRef profile must be %s", ProfileFixture)
	}
	return nil
}

func validateParams(task *agentv1alpha1.AgentTask) error {
	if len(task.Spec.Params) != 1 || task.Spec.Params[0].Name != ParamRequest {
		return fmt.Errorf("profile %s requires only the %s param", ProfileFixture, ParamRequest)
	}
	paramType := task.Spec.Params[0].Type
	if paramType != "" && paramType != pipelinev1.ParamTypeString {
		return fmt.Errorf("profile %s requires a string %s param", ProfileFixture, ParamRequest)
	}
	return nil
}

func validateResults(task *agentv1alpha1.AgentTask) error {
	seen := make(map[string]struct{}, len(task.Spec.Results))
	for _, result := range task.Spec.Results {
		seen[result.Name] = struct{}{}
	}
	if len(seen) != 4 {
		return fmt.Errorf("profile %s requires exactly four results", ProfileFixture)
	}
	for _, name := range []string{ResultOutcome, ResultOutputPVC, ResultOutputPath, ResultOutputDigest} {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("profile %s requires result %s", ProfileFixture, name)
		}
	}
	return nil
}

func requestValue(run *pipelinev1beta1.CustomRun) (string, error) {
	if run == nil {
		return "", fmt.Errorf("custom run is required")
	}
	param := run.Spec.GetParam(ParamRequest)
	if param == nil || param.Value.Type != pipelinev1beta1.ParamTypeString || param.Value.StringVal == "" {
		return "", fmt.Errorf("custom run param %s must be a nonempty string", ParamRequest)
	}
	if !utf8.ValidString(param.Value.StringVal) || len(param.Value.StringVal) > maxRequestBytes {
		return "", fmt.Errorf("custom run param %s must be valid UTF-8 and at most %d bytes", ParamRequest, maxRequestBytes)
	}
	return param.Value.StringVal, nil
}

func executionName(attemptID string) string {
	digest := sha256.Sum256([]byte(attemptID))
	return "fullsend-" + hex.EncodeToString(digest[:10])
}

func outputPath(jobName string) string { return "runs/" + jobName + "/output.txt" }

func correlationLabel(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:10])
}
