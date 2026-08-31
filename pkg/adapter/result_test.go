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
	"strings"
	"testing"
)

const validDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestParseResultRecord(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		record, err := ParseResultRecord(recordJSON(t, nil))
		if err != nil {
			t.Fatalf("ParseResultRecord() error = %v", err)
		}
		if record.ExitCode != 0 || record.Skipped || record.Output.PVC != "agenttask-output" || record.Output.Path != "runs/attempt-0/output" || record.Output.Digest != validDigest {
			t.Fatalf("ParseResultRecord() = %#v", record)
		}
	})

	t.Run("skip", func(t *testing.T) {
		record, err := ParseResultRecord(recordJSON(t, func(value map[string]any) {
			value["skipped"] = true
			value["skipReason"] = "existing change already covers the request"
		}))
		if err != nil {
			t.Fatalf("ParseResultRecord() error = %v", err)
		}
		if !record.Skipped || record.SkipReason == "" || record.ExitCode != 0 {
			t.Fatalf("ParseResultRecord() = %#v", record)
		}
	})

	t.Run("skip without optional reason", func(t *testing.T) {
		record, err := ParseResultRecord(recordJSON(t, func(value map[string]any) {
			value["skipped"] = true
		}))
		if err != nil {
			t.Fatalf("ParseResultRecord() error = %v", err)
		}
		if !record.Skipped || record.SkipReason != "" {
			t.Fatalf("ParseResultRecord() = %#v", record)
		}
	})

	t.Run("nonzero failure", func(t *testing.T) {
		record, err := ParseResultRecord(recordJSON(t, func(value map[string]any) {
			value["exitCode"] = 1
		}))
		if err != nil {
			t.Fatalf("ParseResultRecord() error = %v", err)
		}
		if record.ExitCode != 1 || record.Skipped {
			t.Fatalf("ParseResultRecord() = %#v", record)
		}
	})

	t.Run("benign reason URI", func(t *testing.T) {
		_, err := ParseResultRecord(recordJSON(t, func(value map[string]any) {
			value["skipped"], value["skipReason"] = true, "see https://example.test/change/42"
		}))
		if err != nil {
			t.Fatalf("ParseResultRecord() error = %v", err)
		}
	})
}

func TestParseResultRecordSizeBoundary(t *testing.T) {
	base := recordJSON(t, nil)
	exact := append(append([]byte(nil), base...), bytes.Repeat([]byte{' '}, MaxTerminationRecordBytes-len(base))...)
	if _, err := ParseResultRecord(exact); err != nil {
		t.Fatalf("ParseResultRecord() rejected %d-byte record: %v", len(exact), err)
	}
	if _, err := ParseResultRecord(append(exact, ' ')); err == nil {
		t.Fatalf("ParseResultRecord() accepted %d-byte record", len(exact)+1)
	}
}

func TestParseResultRecordRejectsMalformedEnvelope(t *testing.T) {
	tests := []struct {
		name string
		data func(*testing.T) []byte
	}{
		{name: "empty", data: func(*testing.T) []byte { return nil }},
		{name: "oversized", data: func(t *testing.T) []byte {
			return append(recordJSON(t, nil), []byte(strings.Repeat(" ", MaxTerminationRecordBytes))...)
		}},
		{name: "unknown field", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { value["token"] = "not-allowed" })
		}},
		{name: "unknown output field", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { value["output"].(map[string]any)["url"] = "https://example.com" })
		}},
		{name: "case alias root field", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { value["ExitCode"] = 1 })
		}},
		{name: "case alias output field", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { value["output"].(map[string]any)["PVC"] = "other-output" })
		}},
		{name: "trailing json", data: func(t *testing.T) []byte { return append(recordJSON(t, nil), []byte(` {}`)...) }},
		{name: "duplicate root field", data: func(*testing.T) []byte {
			return []byte(`{"schemaVersion":"fullsend.ai/agenttask-result/v1alpha1","exitCode":0,"exitCode":1,"skipped":false,"skipReason":"","output":{"pvc":"agenttask-output","path":"runs/attempt-0/output","digest":"` + validDigest + `"}}`)
		}},
		{name: "duplicate output field", data: func(*testing.T) []byte {
			return []byte(`{"schemaVersion":"fullsend.ai/agenttask-result/v1alpha1","exitCode":0,"skipped":false,"skipReason":"","output":{"pvc":"agenttask-output","pvc":"other-output","path":"runs/attempt-0/output","digest":"` + validDigest + `"}}`)
		}},
		{name: "invalid utf8", data: func(t *testing.T) []byte {
			data := recordJSON(t, nil)
			return append(data[:len(data)-1], 0xff, '}')
		}},
		{name: "invalid schema", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { value["schemaVersion"] = "fullsend.ai/agenttask-result/v2" })
		}},
		{name: "missing exit", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { delete(value, "exitCode") })
		}},
		{name: "missing skipped", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { delete(value, "skipped") })
		}},
		{name: "missing output", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { delete(value, "output") })
		}},
		{name: "null output", data: func(t *testing.T) []byte {
			return recordJSON(t, func(value map[string]any) { value["output"] = nil })
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseResultRecord(test.data(t)); err == nil {
				t.Fatal("ParseResultRecord() accepted invalid record")
			}
		})
	}
}

func TestParseResultRecordRejectsInvalidStatus(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "negative exit", mutate: func(value map[string]any) { value["exitCode"] = -1 }},
		{name: "exit over 255", mutate: func(value map[string]any) { value["exitCode"] = 256 }},
		{name: "skip with failure exit", mutate: func(value map[string]any) {
			value["exitCode"], value["skipped"], value["skipReason"] = 1, true, "duplicate"
		}},
		{name: "reason without skip", mutate: func(value map[string]any) { value["skipReason"] = "duplicate" }},
		{name: "control character", mutate: func(value map[string]any) {
			value["skipped"], value["skipReason"] = true, "unsafe\nreason"
		}},
		{name: "oversized reason", mutate: func(value map[string]any) {
			value["skipped"], value["skipReason"] = true, strings.Repeat("r", MaxSkipReasonBytes+1)
		}},
		{name: "userinfo in reason URI", mutate: func(value map[string]any) {
			value["skipped"], value["skipReason"] = true, "see https://user:secret@example.test/run"
		}},
		{name: "credential query in reason URI", mutate: func(value map[string]any) {
			value["skipped"], value["skipReason"] = true, "see https://example.test/run?access_token=secret"
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseResultRecord(recordJSON(t, test.mutate)); err == nil {
				t.Fatal("ParseResultRecord() accepted invalid status")
			}
		})
	}
}

func TestParseResultRecordRejectsInvalidOutput(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value string
	}{
		{name: "empty pvc", field: "pvc", value: ""},
		{name: "invalid pvc", field: "pvc", value: "Not_A_Name"},
		{name: "empty path", field: "path", value: ""},
		{name: "absolute path", field: "path", value: "/runs/output"},
		{name: "traversing path", field: "path", value: "runs/../secrets"},
		{name: "backslash path", field: "path", value: `runs\secrets`},
		{name: "control path", field: "path", value: "runs/unsafe\npath"},
		{name: "invalid digest", field: "digest", value: "sha256:abc"},
		{name: "uppercase digest", field: "digest", value: strings.ToUpper(validDigest)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := recordJSON(t, func(value map[string]any) {
				value["output"].(map[string]any)[test.field] = test.value
			})
			if _, err := ParseResultRecord(data); err == nil {
				t.Fatal("ParseResultRecord() accepted invalid output")
			}
		})
	}
}

func recordJSON(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	value := map[string]any{
		"schemaVersion": ResultSchemaVersion,
		"exitCode":      0,
		"skipped":       false,
		"skipReason":    "",
		"output": map[string]any{
			"pvc": "agenttask-output", "path": "runs/attempt-0/output", "digest": validDigest,
		},
	}
	if mutate != nil {
		mutate(value)
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return data
}
