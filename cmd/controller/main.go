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

package main

import (
	"fmt"
	"os"

	"github.com/openshift-pipelines/agenttask-adapter-fullsend/pkg/adapter"
	agentv1alpha1 "github.com/openshift-pipelines/agenttask/api/v1alpha1"
	"github.com/openshift-pipelines/agenttask/pkg/framework"
	pipelinev1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var version = "devel"

func main() {
	ctrl.SetLogger(zap.New())
	image := os.Getenv("FULLSEND_IMAGE")
	if image == "" {
		fail(fmt.Errorf("FULLSEND_IMAGE is required"))
	}

	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"Kubernetes": clientgoscheme.AddToScheme,
		"AgentTask":  agentv1alpha1.AddToScheme,
		"Tekton":     pipelinev1beta1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			fail(fmt.Errorf("register %s scheme: %w", name, err))
		}
	}

	options := ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}}
	if namespace := os.Getenv("POD_NAMESPACE"); namespace != "" {
		options.Cache.DefaultNamespaces = map[string]cache.Config{namespace: {}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		fail(fmt.Errorf("create manager: %w", err))
	}
	implementation := &adapter.Adapter{Client: mgr.GetClient(), Image: image}
	if err := framework.SetupController(mgr, implementation, framework.ControllerOptions{
		Version: version, InstallationID: installationID(),
	}); err != nil {
		fail(fmt.Errorf("setup controller: %w", err))
	}
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		fail(fmt.Errorf("run manager: %w", err))
	}
}

func installationID() string {
	if namespace := os.Getenv("POD_NAMESPACE"); namespace != "" {
		return "fullsend-agenttask-adapter." + namespace
	}
	return "fullsend-agenttask-adapter"
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
