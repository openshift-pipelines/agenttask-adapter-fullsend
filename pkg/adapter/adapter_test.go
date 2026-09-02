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

package adapter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	"github.com/openshift-pipelines/agenttask/pkg/framework"
	pipelinev1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const fixtureImage = "registry.example/fullsend-fixture@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestValidate(t *testing.T) {
	adapter := &Adapter{Client: newFakeClient(t), Image: fixtureImage}
	if err := adapter.Validate(context.Background(), testTask(), testCustomRun()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*agentv1alpha1.AgentTask, *pipelinev1beta1.CustomRun, *Adapter)
		want   error
	}{
		{name: "workspace", mutate: func(task *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun, _ *Adapter) {
			task.Spec.Workspaces = []pipelinev1.WorkspaceDeclaration{{Name: "source"}}
			run.Spec.Workspaces = []pipelinev1beta1.WorkspaceBinding{{Name: "source"}}
		}, want: framework.ErrWorkspaceNotSupported},
		{name: "custom service account", mutate: func(_ *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun, _ *Adapter) {
			run.Spec.ServiceAccountName = "privileged"
		}},
		{name: "wrong profile", mutate: func(task *agentv1alpha1.AgentTask, _ *pipelinev1beta1.CustomRun, _ *Adapter) {
			task.Spec.AdapterRef.Params[0].Value.StringVal = "arbitrary"
		}},
		{name: "missing result", mutate: func(task *agentv1alpha1.AgentTask, _ *pipelinev1beta1.CustomRun, _ *Adapter) {
			task.Spec.Results = task.Spec.Results[:3]
		}},
		{name: "empty request", mutate: func(_ *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun, _ *Adapter) {
			run.Spec.Params[0].Value.StringVal = ""
		}},
		{name: "oversized request", mutate: func(_ *agentv1alpha1.AgentTask, run *pipelinev1beta1.CustomRun, _ *Adapter) {
			run.Spec.Params[0].Value.StringVal = strings.Repeat("x", maxRequestBytes+1)
		}},
		{name: "missing image", mutate: func(_ *agentv1alpha1.AgentTask, _ *pipelinev1beta1.CustomRun, adapter *Adapter) {
			adapter.Image = ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task, run := testTask(), testCustomRun()
			implementation := &Adapter{Client: newFakeClient(t), Image: fixtureImage}
			test.mutate(task, run, implementation)
			err := implementation.Validate(context.Background(), task, run)
			if err == nil {
				t.Fatal("Validate() accepted invalid invocation")
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("Validate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestDesiredJobUsesFixedRestrictedProfile(t *testing.T) {
	adapter := &Adapter{Image: fixtureImage}
	request := testRequest()
	job, err := adapter.desiredJob(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatalf("desiredJob() error = %v", err)
	}
	owner := metav1.GetControllerOf(job)
	podSpec := job.Spec.Template.Spec
	container := podSpec.Containers[0]
	if owner == nil || owner.UID != request.CustomRun.UID || job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("Job ownership or retry policy is invalid: %#v", job)
	}
	if podSpec.ServiceAccountName != RunnerServiceAccount || podSpec.AutomountServiceAccountToken == nil || *podSpec.AutomountServiceAccountToken {
		t.Fatalf("Pod identity = %#v", podSpec)
	}
	if container.Image != fixtureImage || container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("container security = %#v", container)
	}
	if job.Annotations[attemptAnnotation] != request.AttemptID || job.Annotations[profileAnnotation] == "" {
		t.Fatalf("Job annotations = %#v", job.Annotations)
	}
}

func TestReconcileCreatesAdoptsAndCompletesJob(t *testing.T) {
	creates := 0
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&batchv1.Job{}, &corev1.Pod{}).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, inner client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if job, ok := object.(*batchv1.Job); ok {
				creates++
				job.UID = types.UID("job-uid")
			}
			return inner.Create(ctx, object, opts...)
		}}).
		Build()
	adapter := &Adapter{Client: c, Image: fixtureImage}
	request := testRequest()

	observation, err := adapter.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if observation.State != framework.StateAccepted || observation.ExecutionRef == nil || observation.ExecutionRef.UID != "job-uid" {
		t.Fatalf("initial observation = %#v", observation)
	}
	request.ExecutionRef = observation.ExecutionRef
	if _, err := adapter.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("adopt Reconcile() error = %v", err)
	}
	if creates != 1 {
		t.Fatalf("Job creates = %d, want 1", creates)
	}

	var job batchv1.Job
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "test", Name: executionName(request.AttemptID)}, &job); err != nil {
		t.Fatalf("get Job: %v", err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update Job status: %v", err)
	}
	pod := terminatedPod(&job, 0, terminationJSON(t, job.Name, 0, false))
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatalf("create Pod: %v", err)
	}
	observation, err = adapter.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("terminal Reconcile() error = %v", err)
	}
	if observation.State != framework.StateSucceeded || len(observation.Results) != 4 || observation.Results[0].Value != OutcomeCompleted || len(observation.Artifacts) != 1 {
		t.Fatalf("terminal observation = %#v", observation)
	}
}

func TestReconcileAdoptsAfterLostCreateResponse(t *testing.T) {
	lost := true
	creates := 0
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, inner client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if job, ok := object.(*batchv1.Job); ok {
				creates++
				job.UID = "job-uid"
				if err := inner.Create(ctx, object, opts...); err != nil {
					return err
				}
				if lost {
					lost = false
					return errors.New("create response lost")
				}
				return nil
			}
			return inner.Create(ctx, object, opts...)
		},
	}).Build()
	adapter := &Adapter{Client: c, Image: fixtureImage}
	request := testRequest()
	observation, err := adapter.Reconcile(context.Background(), request)
	if err != nil || observation.ExecutionRef == nil || observation.ExecutionRef.UID != "job-uid" {
		t.Fatalf("lost-response adoption = %#v, error = %v", observation, err)
	}
	request.ExecutionRef = observation.ExecutionRef
	if _, err := adapter.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("repeat adoption error = %v", err)
	}
	var jobs batchv1.JobList
	if err := c.List(context.Background(), &jobs); err != nil || len(jobs.Items) != 1 || creates != 1 {
		t.Fatalf("Jobs = %d, creates = %d, error = %v", len(jobs.Items), creates, err)
	}
}

func TestReconcileClassifiesTerminationRecord(t *testing.T) {
	tests := []struct {
		name       string
		exitCode   int32
		message    string
		wantReason string
	}{
		{name: "agent failure", exitCode: 1, message: terminationJSON(t, "placeholder", 1, false), wantReason: framework.ReasonAgentFailed},
		{name: "malformed record", exitCode: 1, message: `{}`, wantReason: framework.ReasonInfrastructureFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := testRequest()
			adapter := &Adapter{Image: fixtureImage}
			job, err := adapter.desiredJob(request, executionName(request.AttemptID))
			if err != nil {
				t.Fatal(err)
			}
			job.UID = "job-uid"
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
			message := test.message
			if test.name == "agent failure" {
				message = terminationJSON(t, job.Name, int(test.exitCode), false)
			}
			pod := terminatedPod(job, test.exitCode, message)
			c := newFakeClient(t, job, pod)
			adapter.Client = c
			observation, err := adapter.Reconcile(context.Background(), request)
			if err != nil || observation.State != framework.StateFailed || observation.Reason != test.wantReason {
				t.Fatalf("observation = %#v, error = %v", observation, err)
			}
		})
	}
}

func TestReconcileRejectsTamperedJob(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*batchv1.Job, *framework.Request)
	}{
		{name: "foreign owner", mutate: func(job *batchv1.Job, _ *framework.Request) { job.OwnerReferences[0].UID = "other-customrun" }},
		{name: "foreign attempt label", mutate: func(job *batchv1.Job, _ *framework.Request) { job.Spec.Template.Labels[attemptLabel] = "other" }},
		{name: "different image", mutate: func(job *batchv1.Job, _ *framework.Request) {
			job.Spec.Template.Spec.Containers[0].Image = "registry.example/other:latest"
		}},
		{name: "command override", mutate: func(job *batchv1.Job, _ *framework.Request) {
			job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/sh"}
		}},
		{name: "init container", mutate: func(job *batchv1.Job, _ *framework.Request) {
			job.Spec.Template.Spec.InitContainers = []corev1.Container{{Name: "inject", Image: fixtureImage}}
		}},
		{name: "environment source", mutate: func(job *batchv1.Job, _ *framework.Request) {
			job.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{}}
		}},
		{name: "log fallback", mutate: func(job *batchv1.Job, _ *framework.Request) {
			job.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		}},
		{name: "unconfined seccomp", mutate: func(job *batchv1.Job, _ *framework.Request) {
			job.Spec.Template.Spec.Containers[0].SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}
		}},
		{name: "different persisted UID", mutate: func(_ *batchv1.Job, request *framework.Request) {
			request.ExecutionRef = &framework.ExecutionReference{APIVersion: batchv1.SchemeGroupVersion.String(), Kind: "Job", Namespace: "test", Name: executionName(request.AttemptID), UID: "other-job"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := testRequest()
			adapter := &Adapter{Image: fixtureImage}
			job, err := adapter.desiredJob(request, executionName(request.AttemptID))
			if err != nil {
				t.Fatal(err)
			}
			job.UID = "job-uid"
			test.mutate(job, &request)
			adapter.Client = newFakeClient(t, job)
			observation, err := adapter.Reconcile(context.Background(), request)
			if err != nil || observation.State != framework.StateFailed || observation.Reason != framework.ReasonInfrastructureFailed {
				t.Fatalf("Reconcile() observation = %#v, error = %v", observation, err)
			}
		})
	}
}

func TestReconcileAllowsAdmissionPullSecretOnPod(t *testing.T) {
	request := testRequest()
	adapter := &Adapter{Image: fixtureImage}
	job, err := adapter.desiredJob(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "job-uid"
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	pod := terminatedPod(job, 0, terminationJSON(t, job.Name, 0, false))
	pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "fullsend-job-dockercfg"}}
	adapter.Client = newFakeClient(t, job, pod)
	observation, err := adapter.Reconcile(context.Background(), request)
	if err != nil || observation.State != framework.StateSucceeded {
		t.Fatalf("Reconcile() observation = %#v, error = %v", observation, err)
	}
}

func TestReconcileRejectsTamperedPod(t *testing.T) {
	request := testRequest()
	adapter := &Adapter{Image: fixtureImage}
	job, err := adapter.desiredJob(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "job-uid"
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	pod := terminatedPod(job, 0, terminationJSON(t, job.Name, 0, false))
	pod.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
	adapter.Client = newFakeClient(t, job, pod)
	observation, err := adapter.Reconcile(context.Background(), request)
	if err != nil || observation.State != framework.StateFailed || observation.Reason != framework.ReasonInfrastructureFailed {
		t.Fatalf("Reconcile() observation = %#v, error = %v", observation, err)
	}
}

func TestSuccessfulObservationMapsSkip(t *testing.T) {
	observation := successfulObservation("test", "pod", &framework.ExecutionReference{Name: "job"}, ResultRecord{
		Skipped: true, Output: OutputReference{PVC: OutputPVCName, Path: "runs/job/output.txt", Digest: validDigest},
	})
	if observation.State != framework.StateSucceeded || observation.Results[0].Value != OutcomeSkipped {
		t.Fatalf("observation = %#v", observation)
	}
}

func TestCancelForegroundDeletesCorrelatedJob(t *testing.T) {
	request := testRequest()
	adapter := &Adapter{Image: fixtureImage}
	job, err := adapter.desiredJob(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "job-uid"
	adapter.Client = newFakeClient(t, job)
	reference, err := executionReference(job)
	if err != nil {
		t.Fatal(err)
	}
	request.ExecutionRef = reference
	request.AgentTask = nil
	request.CustomRun.Spec.Params = nil // deletion reconciliation must not depend on defaulted invocation params
	observation, err := adapter.Cancel(context.Background(), request)
	if err != nil || observation.State != framework.StateCancelling || observation.CleanupComplete {
		t.Fatalf("first Cancel() = %#v, error = %v", observation, err)
	}
	observation, err = adapter.Cancel(context.Background(), request)
	if err != nil || observation.State != framework.StateCancelled || !observation.CleanupComplete {
		t.Fatalf("second Cancel() = %#v, error = %v", observation, err)
	}
}

func TestCancelUsesBoundUIDAcrossImageRotation(t *testing.T) {
	request := testRequest()
	creator := &Adapter{Image: fixtureImage}
	job, err := creator.desiredJob(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "job-uid"
	reference, err := executionReference(job)
	if err != nil {
		t.Fatal(err)
	}
	request.ExecutionRef = reference
	rotated := &Adapter{Client: newFakeClient(t, job), Image: "registry.example/fullsend-fixture@sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}
	observation, err := rotated.Cancel(context.Background(), request)
	if err != nil || observation.State != framework.StateCancelling {
		t.Fatalf("Cancel() = %#v, error = %v", observation, err)
	}
}

func TestCancelWaitsForStableAbsenceAfterUnknownCreate(t *testing.T) {
	request := testRequest()
	request.CustomRun.Status.InitializeConditions()
	request.CustomRun.Status.MarkCustomRunRunning("Cancelling", "cancellation requested")
	started := request.CustomRun.Status.GetCondition(apis.ConditionSucceeded).LastTransitionTime.Inner.Time
	adapter := &Adapter{Client: newFakeClient(t), Image: fixtureImage, Now: func() time.Time { return started.Add(time.Second) }}

	observation, err := adapter.Cancel(context.Background(), request)
	if err != nil || observation.State != framework.StateCancelling || observation.CleanupComplete {
		t.Fatalf("early Cancel() = %#v, error = %v", observation, err)
	}
	adapter.Now = func() time.Time { return started.Add(createSettleTime) }
	observation, err = adapter.Cancel(context.Background(), request)
	if err != nil || observation.State != framework.StateCancelled || !observation.CleanupComplete {
		t.Fatalf("settled Cancel() = %#v, error = %v", observation, err)
	}
}

func TestCancelBoundsStuckCleanup(t *testing.T) {
	request := testRequest()
	request.CustomRun.Status.InitializeConditions()
	request.CustomRun.Status.MarkCustomRunRunning("Cancelling", "cancellation requested")
	started := request.CustomRun.Status.GetCondition(apis.ConditionSucceeded).LastTransitionTime.Inner.Time
	adapter := &Adapter{Image: fixtureImage, Now: func() time.Time { return started.Add(cleanupTimeout) }}
	job, err := adapter.desiredJob(request, executionName(request.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "job-uid"
	adapter.Client = newFakeClient(t, job)
	reference, err := executionReference(job)
	if err != nil {
		t.Fatal(err)
	}
	request.ExecutionRef = reference
	observation, err := adapter.Cancel(context.Background(), request)
	if err != nil || observation.State != framework.StateFailed || observation.Reason != framework.ReasonCleanupFailed || observation.CleanupComplete {
		t.Fatalf("Cancel() = %#v, error = %v", observation, err)
	}
}

func terminatedPod(job *batchv1.Job, exitCode int32, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace, Name: job.Name + "-pod",
			Labels: map[string]string{
				batchv1.ControllerUidLabel: string(job.UID),
				attemptLabel:               job.Labels[attemptLabel],
				customRunUIDLabel:          job.Labels[customRunUIDLabel],
			},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, batchv1.SchemeGroupVersion.WithKind("Job"))},
		},
		Spec: *job.Spec.Template.Spec.DeepCopy(),
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: ContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode, Message: message}},
		}}},
	}
}

func terminationJSON(t *testing.T, jobName string, exitCode int, skipped bool) string {
	t.Helper()
	data := recordJSON(t, func(value map[string]any) {
		value["exitCode"] = exitCode
		value["skipped"] = skipped
		if skipped {
			value["skipReason"] = "fixture skip"
		}
		value["output"].(map[string]any)["path"] = outputPath(jobName)
	})
	return string(data)
}

func testTask() *agentv1alpha1.AgentTask {
	return &agentv1alpha1.AgentTask{
		TypeMeta:   metav1.TypeMeta{APIVersion: agentv1alpha1.SchemeGroupVersion.String(), Kind: "AgentTask"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "fullsend", UID: "task-uid", ResourceVersion: "1"},
		Spec: agentv1alpha1.AgentTaskSpec{
			Params: []pipelinev1.ParamSpec{{Name: ParamRequest, Type: pipelinev1.ParamTypeString}},
			Results: []agentv1alpha1.AgentTaskResult{
				{Name: ResultOutcome}, {Name: ResultOutputPVC}, {Name: ResultOutputPath}, {Name: ResultOutputDigest},
			},
			AdapterRef: agentv1alpha1.AgentTaskAdapterRef{
				Name:   Selector,
				Params: []pipelinev1.Param{{Name: ParamProfile, Value: pipelinev1.ParamValue{Type: pipelinev1.ParamTypeString, StringVal: ProfileFixture}}},
			},
		},
	}
}

func testCustomRun() *pipelinev1beta1.CustomRun {
	return &pipelinev1beta1.CustomRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: pipelinev1beta1.SchemeGroupVersion.String(), Kind: "CustomRun"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "run", UID: types.UID("customrun-uid")},
		Spec: pipelinev1beta1.CustomRunSpec{
			Params: []pipelinev1beta1.Param{{Name: ParamRequest, Value: *pipelinev1beta1.NewStructuredValues("success")}},
		},
	}
}

func testRequest() framework.Request {
	return framework.Request{AgentTask: testTask(), CustomRun: testCustomRun(), AttemptNumber: 0, AttemptID: "customrun-uid:0", ServiceAccountName: "default"}
}

func newFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objects...).Build()
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"kubernetes": clientgoscheme.AddToScheme,
		"agenttask":  agentv1alpha1.AddToScheme,
		"pipeline":   pipelinev1beta1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s scheme: %v", name, err)
		}
	}
	return scheme
}
