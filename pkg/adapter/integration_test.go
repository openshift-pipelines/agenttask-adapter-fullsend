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
	"testing"

	"github.com/openshift-pipelines/agenttask/pkg/framework"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestCustomRunToFullsendJobVerticalSlice(t *testing.T) {
	task := testTask()
	run := testCustomRun()
	run.Spec.CustomRef = &pipelinev1beta1.TaskRef{APIVersion: task.APIVersion, Kind: "AgentTask", Name: task.Name}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithStatusSubresource(&pipelinev1beta1.CustomRun{}, &batchv1.Job{}, &corev1.Pod{}).
		WithObjects(task, run).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, inner client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if job, ok := object.(*batchv1.Job); ok {
				job.UID = types.UID("job-uid")
			}
			return inner.Create(ctx, object, opts...)
		}}).
		Build()
	implementation := &Adapter{Client: c, Image: fixtureImage}
	reconciler := &framework.Reconciler{
		Client: c, Adapter: implementation,
		Options: framework.ControllerOptions{Version: "poc", InstallationID: "test"},
	}
	key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)}
	for i := 0; i < 3; i++ {
		if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
			t.Fatalf("initial Reconcile() call %d error = %v", i+1, err)
		}
	}

	var job batchv1.Job
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "test", Name: executionName("customrun-uid:0")}, &job); err != nil {
		t.Fatalf("get Job: %v", err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update Job status: %v", err)
	}
	if err := c.Create(context.Background(), terminatedPod(&job, 0, terminationJSON(t, job.Name, 0, false))); err != nil {
		t.Fatalf("create Pod: %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background(), key); err != nil {
		t.Fatalf("terminal Reconcile() error = %v", err)
	}

	var completed pipelinev1beta1.CustomRun
	if err := c.Get(context.Background(), key.NamespacedName, &completed); err != nil {
		t.Fatalf("get completed CustomRun: %v", err)
	}
	condition := completed.Status.GetCondition(apis.ConditionSucceeded)
	if condition == nil || !condition.IsTrue() {
		t.Fatalf("CustomRun condition = %#v", condition)
	}
	if len(completed.Status.Results) != 4 || completed.Status.Results[0].Value != OutcomeCompleted {
		t.Fatalf("CustomRun results = %#v", completed.Status.Results)
	}
	profile, err := framework.DecodeStatusProfile(&completed)
	if err != nil || profile.ExecutionRef == nil || profile.ExecutionRef.UID != job.UID || len(profile.Artifacts) != 1 {
		t.Fatalf("status profile = %#v, error = %v", profile, err)
	}
}
