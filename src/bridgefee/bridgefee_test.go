package bridgefee

import (
	"encoding/binary"
	"testing"
)

func TestShouldReplaceGBT_NoSalt(t *testing.T) {
	// When serverSalt is empty, should always return false (disabled)
	result := ShouldReplaceGBT("", 50, []byte("test-job-key"))
	if result {
		t.Error("Expected false when serverSalt is empty")
	}
}

func TestShouldReplaceGBT_ZeroRate(t *testing.T) {
	// When ratePpm is 0, should always return false
	result := ShouldReplaceGBT("test-salt", 0, []byte("test-job-key"))
	if result {
		t.Error("Expected false when ratePpm is 0")
	}
}

func TestShouldReplaceGBT_MaxRate(t *testing.T) {
	// When ratePpm >= 10000, should always return true
	result := ShouldReplaceGBT("test-salt", 10000, []byte("test-job-key"))
	if !result {
		t.Error("Expected true when ratePpm is 10000")
	}

	result = ShouldReplaceGBT("test-salt", 15000, []byte("test-job-key"))
	if !result {
		t.Error("Expected true when ratePpm is > 10000")
	}
}

func TestShouldReplaceGBT_Determinism(t *testing.T) {
	// Same inputs should always produce same result
	salt := "test-salt-123"
	rate := 50
	jobKey := []byte("deterministic-job-key")

	result1 := ShouldReplaceGBT(salt, rate, jobKey)
	result2 := ShouldReplaceGBT(salt, rate, jobKey)
	result3 := ShouldReplaceGBT(salt, rate, jobKey)

	if result1 != result2 || result2 != result3 {
		t.Error("ShouldReplaceGBT is not deterministic")
	}
}

func TestShouldReplaceGBT_DifferentKeys(t *testing.T) {
	// Different job keys should produce different results
	salt := "test-salt-123"
	rate := 500 // Use 5% to increase probability of seeing both

	results := make(map[bool]int)
	for i := 0; i < 1000; i++ {
		jobKey := make([]byte, 8)
		binary.BigEndian.PutUint64(jobKey, uint64(i))
		result := ShouldReplaceGBT(salt, rate, jobKey)
		results[result]++
	}

	// Both true and false should appear (not all same)
	if len(results) < 2 {
		t.Error("Expected both true and false results for different job keys")
	}
}

func TestShouldReplaceGBT_Distribution(t *testing.T) {
	// Test that distribution approximates the configured rate
	salt := "test-salt-456"
	rate := 500 // 5%
	samples := 10000

	trueCount := 0
	for i := 0; i < samples; i++ {
		jobKey := make([]byte, 16)
		binary.BigEndian.PutUint64(jobKey, uint64(i))
		binary.BigEndian.PutUint64(jobKey[8:], uint64(i*7)) // Add some variation
		if ShouldReplaceGBT(salt, rate, jobKey) {
			trueCount++
		}
	}

	// Expected rate is 5% = 500 out of 10000
	// Allow ±20% margin (400-600)
	expectedMin := 400
	expectedMax := 600
	if trueCount < expectedMin || trueCount > expectedMax {
		t.Errorf("Distribution test failed: got %d true results out of %d samples (expected %d-%d)",
			trueCount, samples, expectedMin, expectedMax)
	}
}

func TestShouldReplaceGBT_DifferentSalts(t *testing.T) {
	// Different salts should produce different results for same job key
	rate := 500 // Use 5% to increase probability

	// They might be the same by chance, but test multiple times
	differentResults := false
	for i := 0; i < 100; i++ {
		jobKey := make([]byte, 8)
		binary.BigEndian.PutUint64(jobKey, uint64(i))
		r1 := ShouldReplaceGBT("salt-a", rate, jobKey)
		r2 := ShouldReplaceGBT("salt-b", rate, jobKey)
		if r1 != r2 {
			differentResults = true
			break
		}
	}

	if !differentResults {
		t.Error("Different salts should produce different selection patterns")
	}
}

func TestShouldReplaceGBT_LowRate(t *testing.T) {
	// Test with very low rate (0.5% = 50 ppm)
	salt := "test-salt-789"
	rate := 50 // 0.5%
	samples := 10000

	trueCount := 0
	for i := 0; i < samples; i++ {
		jobKey := make([]byte, 8)
		binary.BigEndian.PutUint64(jobKey, uint64(i))
		if ShouldReplaceGBT(salt, rate, jobKey) {
			trueCount++
		}
	}

	// Expected rate is 0.5% = 50 out of 10000
	// Allow ±50% margin (25-75)
	expectedMin := 25
	expectedMax := 75
	if trueCount < expectedMin || trueCount > expectedMax {
		t.Errorf("Low rate distribution test failed: got %d true results out of %d samples (expected %d-%d)",
			trueCount, samples, expectedMin, expectedMax)
	}
}
