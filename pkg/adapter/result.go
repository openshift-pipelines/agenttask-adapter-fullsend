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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/openshift-pipelines/agenttask/pkg/framework"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	ResultSchemaVersion       = "fullsend.ai/agenttask-result/v1alpha1"
	MaxTerminationRecordBytes = 4096
	MaxSkipReasonBytes        = 512
)

var sha256Pattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

// ResultRecord is the only Fullsend Job output copied from a Pod termination message.
type ResultRecord struct {
	SchemaVersion string
	ExitCode      int
	Skipped       bool
	SkipReason    string
	Output        OutputReference
}

// OutputReference identifies Fullsend output retained on a namespaced PVC.
type OutputReference struct {
	PVC    string
	Path   string
	Digest string
}

type wireResultRecord struct {
	SchemaVersion string               `json:"schemaVersion"`
	ExitCode      *int                 `json:"exitCode"`
	Skipped       *bool                `json:"skipped"`
	SkipReason    string               `json:"skipReason"`
	Output        *wireOutputReference `json:"output"`
}

type wireOutputReference struct {
	PVC    string `json:"pvc"`
	Path   string `json:"path"`
	Digest string `json:"digest"`
}

// ParseResultRecord parses one size-bounded JSON record and rejects ambiguous input.
func ParseResultRecord(data []byte) (ResultRecord, error) {
	if len(data) == 0 {
		return ResultRecord{}, fmt.Errorf("termination record is empty")
	}
	if len(data) > MaxTerminationRecordBytes {
		return ResultRecord{}, fmt.Errorf("termination record exceeds %d bytes", MaxTerminationRecordBytes)
	}
	if !utf8.Valid(data) {
		return ResultRecord{}, fmt.Errorf("termination record must be valid UTF-8")
	}
	if err := validateResultJSONShape(data); err != nil {
		return ResultRecord{}, err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire wireResultRecord
	if err := decoder.Decode(&wire); err != nil {
		return ResultRecord{}, fmt.Errorf("decode termination record: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return ResultRecord{}, err
	}
	if wire.SchemaVersion != ResultSchemaVersion {
		return ResultRecord{}, fmt.Errorf("unsupported termination record schema")
	}
	if wire.ExitCode == nil {
		return ResultRecord{}, fmt.Errorf("exitCode is required")
	}
	if *wire.ExitCode < 0 || *wire.ExitCode > 255 {
		return ResultRecord{}, fmt.Errorf("exitCode must be between 0 and 255")
	}
	if wire.Skipped == nil {
		return ResultRecord{}, fmt.Errorf("skipped is required")
	}
	if err := validateSkip(*wire.Skipped, *wire.ExitCode, wire.SkipReason); err != nil {
		return ResultRecord{}, err
	}
	if wire.Output == nil {
		return ResultRecord{}, fmt.Errorf("output is required")
	}
	if err := validateOutput(*wire.Output); err != nil {
		return ResultRecord{}, err
	}

	return ResultRecord{
		SchemaVersion: wire.SchemaVersion,
		ExitCode:      *wire.ExitCode,
		Skipped:       *wire.Skipped,
		SkipReason:    wire.SkipReason,
		Output: OutputReference{
			PVC: wire.Output.PVC, Path: wire.Output.Path, Digest: wire.Output.Digest,
		},
	}, nil
}

type jsonObjectShape map[string]jsonObjectShape

func validateResultJSONShape(data []byte) error {
	outputFields := jsonObjectShape{
		"pvc": nil, "path": nil, "digest": nil,
	}
	rootFields := jsonObjectShape{
		"schemaVersion": nil,
		"exitCode":      nil,
		"skipped":       nil,
		"skipReason":    nil,
		"output":        outputFields,
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := validateJSONObject(decoder, rootFields); err != nil {
		return fmt.Errorf("validate termination record JSON: %w", err)
	}
	return requireJSONEOF(decoder)
}

func validateJSONObject(decoder *json.Decoder, fields jsonObjectShape) error {
	opening, err := decoder.Token()
	if err != nil {
		return err
	}
	if opening != json.Delim('{') {
		return fmt.Errorf("expected JSON object")
	}

	seen := make(map[string]struct{}, len(fields))
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("object key is not a string")
		}
		nested, allowed := fields[key]
		if !allowed {
			return fmt.Errorf("unknown JSON field")
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate JSON field")
		}
		seen[key] = struct{}{}
		if nested != nil {
			if err := validateJSONObject(decoder, nested); err != nil {
				return err
			}
			continue
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim('}') {
		return fmt.Errorf("expected end of JSON object")
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("termination record contains trailing JSON")
		}
		return fmt.Errorf("decode trailing termination record data: %w", err)
	}
	return nil
}

func validateSkip(skipped bool, exitCode int, reason string) error {
	if !utf8.ValidString(reason) || containsControl(reason) {
		return fmt.Errorf("skipReason must be valid text without control characters")
	}
	if len(reason) > MaxSkipReasonBytes {
		return fmt.Errorf("skipReason exceeds %d bytes", MaxSkipReasonBytes)
	}
	if err := validateCredentialFreeText(reason); err != nil {
		return err
	}
	if skipped {
		if exitCode != 0 {
			return fmt.Errorf("a skipped run must have exitCode 0")
		}
	} else if reason != "" {
		return fmt.Errorf("skipReason is only valid for a skipped run")
	}
	return nil
}

func validateCredentialFreeText(value string) error {
	for _, token := range strings.Fields(value) {
		if !strings.Contains(token, "://") {
			continue
		}
		candidate := strings.Trim(token, `.,;!?()[]{}<>"'`)
		if err := framework.ValidateCredentialFreeURI(candidate); err != nil {
			return fmt.Errorf("skipReason must not contain an unsafe URI")
		}
	}
	return nil
}

func validateOutput(output wireOutputReference) error {
	if errs := validation.IsDNS1123Subdomain(output.PVC); len(errs) != 0 {
		return fmt.Errorf("output pvc must be a DNS subdomain")
	}
	if output.Path == "" || path.IsAbs(output.Path) || path.Clean(output.Path) != output.Path || output.Path == "." || strings.Contains(output.Path, `\`) || containsControl(output.Path) {
		return fmt.Errorf("output path must be a clean relative path")
	}
	for _, part := range strings.Split(output.Path, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("output path must not traverse directories")
		}
	}
	if !sha256Pattern.MatchString(output.Digest) {
		return fmt.Errorf("output digest must be a lowercase sha256 digest")
	}
	return nil
}

func containsControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
