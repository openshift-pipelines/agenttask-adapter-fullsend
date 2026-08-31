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
	"errors"

	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	"github.com/openshift-pipelines/agenttask/pkg/framework"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
)

const Selector = "fullsend.ai/agenttask-adapter"

// ErrNotImplemented keeps the scaffold fail-closed until the Job lifecycle exists.
var ErrNotImplemented = errors.New("fullsend adapter lifecycle is not implemented")

// Adapter is a fail-closed shell for the Fullsend integration.
type Adapter struct{}

var _ framework.AgentTaskAdapter = (*Adapter)(nil)

// Name returns the selector declared by AgentTask definitions.
func (*Adapter) Name(context.Context) string { return Selector }

// Validate rejects every execution until the Fullsend Job contract is implemented.
func (*Adapter) Validate(context.Context, *agentv1alpha1.AgentTask, *pipelinev1beta1.CustomRun) error {
	return ErrNotImplemented
}

// Reconcile rejects every execution without creating or mutating native resources.
func (*Adapter) Reconcile(context.Context, framework.Request) (framework.Observation, error) {
	return framework.Observation{}, ErrNotImplemented
}

// Cancel rejects every request without creating or mutating native resources.
func (*Adapter) Cancel(context.Context, framework.Request) (framework.Observation, error) {
	return framework.Observation{}, ErrNotImplemented
}
