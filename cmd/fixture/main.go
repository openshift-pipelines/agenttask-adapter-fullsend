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

// Command fixture implements the bounded Fullsend Job result contract for E2E tests.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const schemaVersion = "fullsend.ai/agenttask-result/v1alpha1"

type resultRecord struct {
	SchemaVersion string          `json:"schemaVersion"`
	ExitCode      int             `json:"exitCode"`
	Skipped       bool            `json:"skipped"`
	SkipReason    string          `json:"skipReason"`
	Output        outputReference `json:"output"`
}

type outputReference struct {
	PVC    string `json:"pvc"`
	Path   string `json:"path"`
	Digest string `json:"digest"`
}

func main() {
	if os.Getenv("AGENTTASK_REQUEST") == "sleep" {
		time.Sleep(10 * time.Minute)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fixture failed")
		os.Exit(2)
	}
}

func run() error {
	attempt := os.Getenv("AGENTTASK_ATTEMPT_ID")
	request := os.Getenv("AGENTTASK_REQUEST")
	pvc := os.Getenv("FULLSEND_OUTPUT_PVC")
	relativePath := os.Getenv("FULLSEND_OUTPUT_PATH")
	root := os.Getenv("FULLSEND_OUTPUT_ROOT")
	if attempt == "" || request == "" || pvc == "" || root == "" || relativePath == "" || filepath.IsAbs(relativePath) ||
		filepath.Clean(relativePath) != relativePath || relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return fmt.Errorf("invalid fixture configuration")
	}

	content := []byte("fullsend fixture result for " + attempt + "\n")
	outputFile := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(outputFile), 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(outputFile, content, 0o640); err != nil {
		return err
	}
	digest := sha256.Sum256(content)
	record := resultRecord{
		SchemaVersion: schemaVersion,
		Output:        outputReference{PVC: pvc, Path: relativePath, Digest: "sha256:" + hex.EncodeToString(digest[:])},
	}
	switch request {
	case "skip":
		record.Skipped = true
		record.SkipReason = "fixture requested a skip"
	case "fail":
		record.ExitCode = 1
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := os.WriteFile("/dev/termination-log", data, 0o600); err != nil {
		return err
	}
	if record.ExitCode != 0 {
		os.Exit(record.ExitCode)
	}
	return nil
}
