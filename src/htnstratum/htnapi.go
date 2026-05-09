package htnstratum

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoosat-Oy/HTND/app/appmessage"
	"github.com/Hoosat-Oy/HTND/infrastructure/network/rpcclient"
	"github.com/Hoosat-Oy/htn-stratum-bridge/src/gostratum"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// gbtCacheEntry holds a cached GetBlockTemplate response and the time it was fetched.
type gbtCacheEntry struct {
	template  *appmessage.GetBlockTemplateResponseMessage
	fetchedAt time.Time
}

type HtnApi struct {
	address       string
	blockWaitTime time.Duration
	logger        *zap.SugaredLogger
	hoosat        *rpcclient.RPCClient
	connected     bool

	// synced tracks the latest observed node sync status from GetInfo().
	// It is updated periodically by the block template listener.
	synced atomic.Bool

	// GBT response cache (per payout address)
	gbtCache    map[string]*gbtCacheEntry
	gbtCacheTTL time.Duration
	gbtCacheMu  sync.Mutex

	// Stats
	gbtCacheHits   uint64
	gbtCacheMisses uint64

	// DO NOT LET GBT RACE!!!
	nodeCallMu sync.Mutex
}

func NewHoosatAPI(address string, blockWaitTime time.Duration, logger *zap.SugaredLogger, gbtCacheTTL time.Duration) (*HtnApi, error) {
	client, err := rpcclient.NewRPCClient(address)
	if err != nil {
		return nil, err
	}

	return &HtnApi{
		address:       address,
		blockWaitTime: blockWaitTime,
		logger:        logger.With(zap.String("component", "hoosatapi:"+address)),
		hoosat:        client,
		connected:     true,
		gbtCache:      make(map[string]*gbtCacheEntry),
		gbtCacheTTL:   gbtCacheTTL,
	}, nil
}

func (htnApi *HtnApi) Start(ctx context.Context, cfg BridgeConfig, blockCb func()) {
	if !cfg.MineWhenNotSynced {
		if err := htnApi.waitForSync(ctx, true); err != nil {
			htnApi.logger.Warn("sync wait interrupted; bridge may not produce work", zap.Error(err))
			return
		}
	}
	go htnApi.startBlockTemplateListener(ctx, cfg.MineWhenNotSynced, blockCb)
	go htnApi.startStatsThread(ctx)
	go htnApi.startCacheCleanupThread(ctx)
}

func (htnApi *HtnApi) IsSynced() bool {
	return htnApi.synced.Load()
}

func (htnApi *HtnApi) checkSyncState() (bool, error) {
	clientInfo, err := htnApi.hoosat.GetInfo()
	if err != nil {
		return false, err
	}
	htnApi.synced.Store(clientInfo.IsSynced)
	return clientInfo.IsSynced, nil
}

func (htnApi *HtnApi) startStatsThread(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			htnApi.logger.Warn("context cancelled, stopping stats thread")
			return
		case <-ticker.C:
			dagResponse, err := htnApi.hoosat.GetBlockDAGInfo()
			if err != nil {
				htnApi.logger.Warn("failed to get network hashrate from hoosat, prom stats will be out of date", zap.Error(err))
				continue
			}
			response, err := htnApi.hoosat.EstimateNetworkHashesPerSecond(dagResponse.TipHashes[0], 1000)
			if err != nil {
				htnApi.logger.Warn("failed to get network hashrate from hoosat, prom stats will be out of date", zap.Error(err))
				continue
			}
			RecordNetworkStats(response.NetworkHashesPerSecond, dagResponse.BlockCount, dagResponse.Difficulty)
		}
	}
}

// startCacheCleanupThread periodically evicts expired GBT cache entries so
// the cache map does not grow unbounded when many distinct payout addresses
// are seen over time.  It is a no-op when caching is disabled (TTL == 0).
func (htnApi *HtnApi) startCacheCleanupThread(ctx context.Context) {
	if htnApi.gbtCacheTTL == 0 {
		return
	}
	ticker := time.NewTicker(htnApi.gbtCacheTTL * 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			htnApi.gbtCacheMu.Lock()
			for addr, entry := range htnApi.gbtCache {
				if time.Since(entry.fetchedAt) >= htnApi.gbtCacheTTL {
					delete(htnApi.gbtCache, addr)
				}
			}
			htnApi.gbtCacheMu.Unlock()
		}
	}
}

func (htnApi *HtnApi) invalidateGBTCache() {
	// If caching is disabled, nothing to do.
	if htnApi.gbtCacheTTL == 0 {
		return
	}

	htnApi.gbtCacheMu.Lock()
	clear(htnApi.gbtCache)
	htnApi.gbtCacheMu.Unlock()
}

func (htnApi *HtnApi) reconnect() error {
	if htnApi.hoosat != nil {
		return htnApi.hoosat.Reconnect()
	}

	client, err := rpcclient.NewRPCClient(htnApi.address)
	if err != nil {
		return err
	}
	htnApi.hoosat = client
	htnApi.invalidateGBTCache()
	return nil
}

func (htnApi *HtnApi) waitForSync(ctx context.Context, verbose bool) error {
	if verbose {
		htnApi.logger.Info("checking hoosat sync state")
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		synced, err := htnApi.checkSyncState()
		if err == nil && synced {
			if verbose {
				htnApi.logger.Info("HTN synced, starting server")
			}
			return nil
		}
		if err != nil {
			htnApi.logger.Warn("failed to check HTN sync state", zap.Error(err))
			if err := htnApi.reconnect(); err != nil {
				htnApi.logger.Warn("failed to reconnect to hoosat", zap.Error(err))
			}
		} else {
			htnApi.logger.Warn("HTN is not synced, waiting for sync before starting bridge")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (htnApi *HtnApi) startBlockTemplateListener(ctx context.Context, mineWhenNotSynced bool, blockReadyCb func()) {
	// Buffer + non-blocking send so notification callbacks never deadlock
	// when the listener is temporarily paused.
	blockReadyChan := make(chan struct{}, 1)
	err := htnApi.hoosat.RegisterForNewBlockTemplateNotifications(func(_ *appmessage.NewBlockTemplateNotificationMessage) {
		select {
		case blockReadyChan <- struct{}{}:
		default:
		}
	})
	if err != nil {
		htnApi.logger.Error("fatal: failed to register for block notifications from hoosat")
	}

	blockTicker := time.NewTicker(htnApi.blockWaitTime)
	defer blockTicker.Stop()
	syncTicker := time.NewTicker(5 * time.Second)
	defer syncTicker.Stop()

	// Prime sync state.
	if _, err := htnApi.checkSyncState(); err != nil {
		htnApi.logger.Warn("initial sync check failed", zap.Error(err))
		if err := htnApi.reconnect(); err != nil {
			htnApi.logger.Warn("initial reconnect failed", zap.Error(err))
		}
	}
	prevSynced := htnApi.IsSynced()
	lastNotSyncedLog := time.Now().Add(-time.Hour)

	for {
		select {
		case <-ctx.Done():
			htnApi.logger.Warn("context cancelled, stopping block update listener")
			return
		case <-syncTicker.C:
			synced, err := htnApi.checkSyncState()
			if err != nil {
				htnApi.logger.Warn("error checking hoosat sync state, attempting reconnect", zap.Error(err))
				if err := htnApi.reconnect(); err != nil {
					htnApi.logger.Warn("error reconnecting to hoosat", zap.Error(err))
				}
				continue
			}
			if synced && !prevSynced && !mineWhenNotSynced {
				// Node just became synced; nudge a fresh job out immediately.
				blockReadyCb()
				blockTicker.Reset(htnApi.blockWaitTime)
			}
			prevSynced = synced
		case <-blockReadyChan:
			if mineWhenNotSynced || htnApi.IsSynced() {
				blockReadyCb()
				blockTicker.Reset(htnApi.blockWaitTime)
				continue
			}
			if time.Since(lastNotSyncedLog) > 30*time.Second {
				htnApi.logger.Warn("HTN is not synced; mining is paused (set mine_when_not_synced: true to override)")
				lastNotSyncedLog = time.Now()
			}
		case <-blockTicker.C: // timeout, manually check for new blocks
			if mineWhenNotSynced || htnApi.IsSynced() {
				blockReadyCb()
				continue
			}
			if time.Since(lastNotSyncedLog) > 30*time.Second {
				htnApi.logger.Warn("HTN is not synced; mining is paused (set mine_when_not_synced: true to override)")
				lastNotSyncedLog = time.Now()
			}
		}
	}
}

func sanitizeWorkerID(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	s = strings.ReplaceAll(s, " ", "_")
	re := regexp.MustCompile(`[^a-zA-Z0-9._-]`)
	s = re.ReplaceAllString(s, "")
	if len(s) > 32 {
		s = s[:32]
	}
	return s
}

func (htnApi *HtnApi) GetBlockTemplate(client *gostratum.StratumContext, poll int64, vote int64) (*appmessage.GetBlockTemplateResponseMessage, error) {
	payoutAddress := client.WalletAddr

	// Build extraData string.
	// For normal mining when caching is enabled, we keep extraData constant so multiple
	// workers can share the cached template for the same payout address.
	// For poll/vote we preserve the previous behavior (includes worker attribution).
	var extraData string
	if poll != 0 && vote != 0 {
		extraData = fmt.Sprintf(`'%s' via htn-stratum-bridge_%s as worker %s poll %d vote %d`,
			client.RemoteApp, version, sanitizeWorkerID(client.WorkerName), poll, vote)
	} else if htnApi.gbtCacheTTL > 0 {
		extraData = fmt.Sprintf(`Mined via htn-stratum-bridge version %s`, version)
	} else {
		extraData = fmt.Sprintf(`'%s' via htn-stratum-bridge_%s as worker %s`,
			client.RemoteApp, version, sanitizeWorkerID(client.WorkerName))
	}

	// Use cache only for normal mining templates (poll/vote templates must remain uncached,
	// because their extraData differs).
	if htnApi.gbtCacheTTL > 0 && poll == 0 && vote == 0 {
		htnApi.gbtCacheMu.Lock()
		entry, ok := htnApi.gbtCache[payoutAddress]
		if ok && time.Since(entry.fetchedAt) < htnApi.gbtCacheTTL {
			cached := entry.template
			htnApi.gbtCacheMu.Unlock()

			hits := atomic.AddUint64(&htnApi.gbtCacheHits, 1)
			misses := atomic.LoadUint64(&htnApi.gbtCacheMisses)
			total := hits + misses
			if total%1000 == 0 {
				rate := (float64(hits) / float64(total)) * 100.0
				htnApi.logger.Debug("GBT cache hit rate %.2f%% (%d/%d), ttl=%s", rate, hits, total, htnApi.gbtCacheTTL)
			}
			return cached, nil
		}
		htnApi.gbtCacheMu.Unlock()
		atomic.AddUint64(&htnApi.gbtCacheMisses, 1)

		// Cache miss or expired – fetch outside the lock.
		// BUT STILL STOP RACING!!
		htnApi.nodeCallMu.Lock()
		template, err := htnApi.hoosat.GetBlockTemplate(payoutAddress, extraData)
		htnApi.nodeCallMu.Unlock()

		if err != nil {
			return nil, errors.Wrap(err, "failed fetching new block template from hoosat")
		}
		htnApi.gbtCacheMu.Lock()
		htnApi.gbtCache[payoutAddress] = &gbtCacheEntry{template: template, fetchedAt: time.Now()}
		htnApi.gbtCacheMu.Unlock()
		return template, nil
	}

	// Cache disabled or poll/vote path.
	// This has never had a lock, but it needs one to stop cross pollution of GBTs
	htnApi.nodeCallMu.Lock()
	template, err := htnApi.hoosat.GetBlockTemplate(payoutAddress, extraData)
	htnApi.nodeCallMu.Unlock()

	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from hoosat")
	}
	return template, nil
}
