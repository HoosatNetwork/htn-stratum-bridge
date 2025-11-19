# Bridge Fee Feature Documentation

## Overview

The bridge fee feature allows bridge operators to optionally divert a small, configurable percentage of block templates to a bridge address. This feature is **disabled by default** and requires explicit configuration to activate.

## How It Works

### Request Modification
When enabled, the bridge modifies the **input** to `getblocktemplate` requests sent to the daemon:
- Instead of always using the miner's payout address, the bridge conditionally uses the configured bridge address
- The daemon generates the block template with the bridge address in the coinbase
- Miners receive work normally and submit shares as usual
- No changes are made to the miner submission flow

### Deterministic Selection
Selection uses HMAC-SHA256 with a server-side secret to ensure:
- **Deterministic**: Same inputs always produce same result
- **Server-controlled**: Miners cannot manipulate selection
- **Transparent**: Logged and metrics tracked for auditing

### Job Key Construction
Each GBT request is identified by a unique job key:
```
jobKey = jobCounter (8 bytes) || prevBlockHash (32 bytes) || timestamp (8 bytes) || workerID (variable)
```

The selection algorithm:
1. Compute `HMAC-SHA256(serverSalt, jobKey)`
2. Interpret first 8 bytes as big-endian uint64 `v`
3. Return true if `v % 10000 < ratePpm`

This ensures approximately `ratePpm / 10000` of requests are diverted.

## Configuration

### config.yaml
```yaml
bridge_fee:
  # enabled: Set to true to enable the feature (default: false)
  enabled: false
  
  # rate_ppm: Parts per 10000 to divert (default: 50 = 0.5%)
  # Examples: 50 = 0.5%, 100 = 1%, 500 = 5%
  rate_ppm: 50
  
  # address: Bridge payout address
  address: "hoosat:qq2g85qrj2k4xs80y32v69kjn7nr49khyrack9mpd3gy54vfp8ja53ws4yezz"
  
  # server_salt: REQUIRED secret salt for deterministic selection
  # Generate with: openssl rand -hex 32
  server_salt: ""
```

### Important Notes
- **`server_salt` is required**: Without it, the feature remains disabled even if `enabled: true`
- **Keep `server_salt` secret**: This prevents miners from gaming the selection
- **Default rate**: 0.5% (50 parts per 10,000) when enabled
- **Default address**: Pre-configured bridge address

## Logging

When a GBT is diverted, an info-level log entry is created:
```
INFO diverting GBT to bridge address
  job_counter: 12345
  prev_block_hash: abc123...
  worker: worker-name
```

## Metrics

Prometheus counter tracking diverted requests:
```
# HELP htn_diverted_gbt_total Total number of GBT requests diverted to bridge address
# TYPE htn_diverted_gbt_total counter
htn_diverted_gbt_total 42
```

Query with:
```bash
curl http://localhost:2114/metrics | grep htn_diverted_gbt_total
```

## Security

### Server Salt
- **Required for activation**: Feature is effectively disabled without it
- **Generate randomly**: Use `openssl rand -hex 32` or similar
- **Keep private**: Do not share or commit to version control
- **Unique per bridge**: Each bridge instance should use a different salt

### Selection Algorithm
- **HMAC-SHA256**: Cryptographically secure deterministic selection
- **Server-side only**: Miners cannot influence which jobs are diverted
- **Transparent**: All diversions are logged and counted

### Error Handling
- If `prevBlockHash` decode fails, the selection is skipped for that GBT (logged as warning)
- If DAG info fetch fails, the selection is skipped (logged as warning)
- The bridge continues operating normally even if selection fails

## Testing

### Unit Tests (8 tests in src/bridgefee/)
- Determinism (same inputs → same outputs)
- Distribution (approximates configured rate)
- Edge cases (no salt, zero rate, max rate)
- Different salts produce different patterns

### Integration Tests (5 tests in src/htnstratum/)
- Configuration loading and defaults
- Feature disabled behavior
- Server salt requirement
- Job counter increment

Run tests:
```bash
go test ./src/bridgefee/... ./src/htnstratum/... -v
```

## Production Deployment

### Checklist
1. ✅ Generate a secure random `server_salt`
2. ✅ Configure `rate_ppm` based on desired percentage
3. ✅ Set `enabled: true` in config.yaml
4. ✅ Verify bridge address is correct
5. ✅ Monitor `htn_diverted_gbt_total` metric
6. ✅ Check logs for diversion events

### Monitoring
Monitor these metrics:
- `htn_diverted_gbt_total`: Total diversions
- Calculate rate: `rate(htn_diverted_gbt_total[5m]) / rate(htn_worker_job_counter[5m])`

### Troubleshooting

**Feature not working even though enabled?**
- Check that `server_salt` is configured (not empty)
- Verify logs show "Bridge fee enabled: true" at startup
- Check for warnings about ServerSalt

**Want to verify selection is working?**
- Set `rate_ppm: 10000` temporarily (diverts 100% of requests)
- Monitor logs for "diverting GBT to bridge address" messages
- Check `htn_diverted_gbt_total` metric increases
- Set rate back to desired value

**Concerned about miner experience?**
- Miners receive work normally, unaware of selection
- No changes to share submission or difficulty
- Only affects which address receives block reward for diverted jobs

## Implementation Details

### Files Added/Modified
- `src/bridgefee/bridgefee.go`: Selection logic
- `src/bridgefee/bridgefee_test.go`: Unit tests
- `src/htnstratum/htnapi.go`: GBT request modification
- `src/htnstratum/stratum_server.go`: Configuration loading
- `src/htnstratum/prom.go`: Metrics
- `src/htnstratum/bridgefee_integration_test.go`: Integration tests
- `cmd/htnbridge/config.yaml`: Configuration example
- `cmd/htnbridge/main.go`: Startup logging
- `README.md`: User documentation

### Performance Impact
- **Minimal**: HMAC-SHA256 computation adds ~microseconds per GBT
- **Optimized**: Pre-allocated job key buffer, direct byte operations
- **No miner impact**: Selection happens server-side before work is sent

### Thread Safety
- Job counter is atomic (`sync/atomic.AddUint64`)
- HMAC computation is per-request (no shared state)
- No locks needed for selection logic

## License & Attribution

Part of htn-stratum-bridge
https://github.com/MattF42/htn-stratum-bridge
