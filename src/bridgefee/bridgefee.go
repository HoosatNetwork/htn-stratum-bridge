package bridgefee

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// ShouldReplaceGBT determines whether a given GBT request should use the bridge
// address instead of the miner's address based on deterministic server-side selection.
//
// Parameters:
//   - serverSalt: Secret salt for HMAC (must be configured in production)
//   - ratePpm: Parts per 10000 (e.g., 50 = 0.5%)
//   - jobKey: Deterministic job identifier (jobCounter || prevBlockHash || timestamp || workerID)
//
// Returns true if this job should be diverted to the bridge address.
func ShouldReplaceGBT(serverSalt string, ratePpm int, jobKey []byte) bool {
	// If no server salt configured, feature is disabled
	if serverSalt == "" {
		return false
	}

	// If rate is 0 or negative, never replace
	if ratePpm <= 0 {
		return false
	}

	// If rate is >= 10000, always replace
	if ratePpm >= 10000 {
		return true
	}

	// Compute HMAC-SHA256
	h := hmac.New(sha256.New, []byte(serverSalt))
	h.Write(jobKey)
	hash := h.Sum(nil)

	// Interpret first 8 bytes as big-endian uint64
	value := binary.BigEndian.Uint64(hash[:8])

	// Return true if (value % 10000) < ratePpm
	return (value % 10000) < uint64(ratePpm)
}
