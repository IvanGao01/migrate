package main

import (
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"testing"
)

func TestParseValues(t *testing.T) {
	// Test case 1: When result has no data (nil series)
	result := []client.Result{
		{Series: nil, Messages: nil, Err: ""},
	}
	expected := map[string]string(nil)
	if got := ParseValues(result); !areMapsEqual(got, expected) {
		t.Errorf("Test case 1 failed. Expected: %v, Got: %v", expected, got)
	}

	// Test case 2: When result has data
	result = []client.Result{
		{Series: []models.Row{
			{
				Name:    "cpu",
				Tags:    map[string]string{},
				Columns: []string{"fieldKey", "fieldType"},
				Values: [][]interface{}{
					{"usage_guest", "float"},
					{"usage_guest_nice", "float"},
					{"usage_idle", "float"},
					{"usage_iowait", "float"},
					{"usage_irq", "float"},
					{"usage_nice", "float"},
					{"usage_softirq", "float"},
					{"usage_steal", "float"},
					{"usage_system", "float"},
					{"usage_user", "float"},
				},
				Partial: false,
			},
		}, Messages: nil, Err: ""},
	}

	expected = map[string]string{
		"usage_guest":      "float",
		"usage_guest_nice": "float",
		"usage_idle":       "float",
		"usage_iowait":     "float",
		"usage_irq":        "float",
		"usage_nice":       "float",
		"usage_softirq":    "float",
		"usage_steal":      "float",
		"usage_system":     "float",
		"usage_user":       "float",
	}

	if got := ParseValues(result); !areMapsEqual(got, expected) {
		t.Errorf("Test case 2 failed. Expected: %v, Got: %v", expected, got)
	}
}

func TestParseTags(t *testing.T) {
	// Test case 1: When result has no data (nil series)
	result := []client.Result{
		{Series: nil, Messages: nil, Err: ""},
	}
	expected := []string(nil)
	if got := ParseTags(result); !areStringSlicesEqual(got, expected) {
		t.Errorf("Test case 1 failed. Expected: %v, Got: %v", expected, got)
	}

	// Test case 2: When result has data
	result = []client.Result{
		{Series: []models.Row{
			{
				Name:    "cpu",
				Tags:    nil,
				Columns: []string{"tagKey"},
				Values: [][]interface {
				}{
					{"cpu"},
					{"host"},
				},
				Partial: false,
			},
		}, Messages: nil, Err: ""},
	}

	expected = []string{"cpu", "host"}

	if got := ParseTags(result); !areStringSlicesEqual(got, expected) {
		t.Errorf("Test case 2 failed. Expected: %v, Got: %v", expected, got)
	}
}

// Helper function to compare maps
func areMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bVal, ok := b[k]; !ok || v != bVal {
			return false
		}
	}
	return true
}

// Helper function to compare string slices
func areStringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
