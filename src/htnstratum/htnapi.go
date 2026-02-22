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

	// GBT response cache (per payout address)
	gbtCache    map[string]*gbtCacheEntry
	gbtCacheTTL time.Duration
	gbtCacheMu  sync.Mutex

	// Stats
	gbtCacheHits   uint64
	gbtCacheMisses uint64
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
		htnApi.waitForSync(true)
	}
	go htnApi.startBlockTemplateListener(ctx, blockCb)
	go htnApi.startStatsThread(ctx)
	go htnApi.startCacheCleanupThread(ctx)
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

func (htnApi *HtnApi) waitForSync(verbose bool) error {
	if verbose {
		htnApi.logger.Info("checking hoosat sync state")
	}
	for {
		clientInfo, err := htnApi.hoosat.GetInfo()
		if err != nil {
			return errors.Wrapf(err, "error fetching server info from hoosat @ %s", htnApi.address)
		}
		if clientInfo.IsSynced {
			break
		}
		htnApi.logger.Warn("HTN is not synced, waiting for sync before starting bridge")
		time.Sleep(5 * time.Second)
	}
	if verbose {
		htnApi.logger.Info("HTN synced, starting server")
	}
	return nil
}

func (htnApi *HtnApi) startBlockTemplateListener(ctx context.Context, blockReadyCb func()) {
	blockReadyChan := make(chan bool)
	err := htnApi.hoosat.RegisterForNewBlockTemplateNotifications(func(_ *appmessage.NewBlockTemplateNotificationMessage) {
		// htnApi.invalidateGBTCache() // Happens far too often, and not just for new Tips.  Instead rely on 100ms TTL
		blockReadyChan <- true
	})
	if err != nil {
		htnApi.logger.Error("fatal: failed to register for block notifications from hoosat")
	}

	ticker := time.NewTicker(htnApi.blockWaitTime)
	for {
		if err := htnApi.waitForSync(false); err != nil {
			htnApi.logger.Error("error checking hoosat sync state, attempting reconnect: ", err)
			if err := htnApi.reconnect(); err != nil {
				htnApi.logger.Error("error reconnecting to hoosat, waiting before retry: ", err)
				time.Sleep(15 * time.Second)
			}
		}
		select {
		case <-ctx.Done():
			htnApi.logger.Warn("context cancelled, stopping block update listener")
			return
		case <-blockReadyChan:
			blockReadyCb()
			ticker.Reset(htnApi.blockWaitTime)
		case <-ticker.C: // timeout, manually check for new blocks
			blockReadyCb()
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
			        htnApi.logger.Infof("GBT cache hit rate %.2f%% (%d/%d), ttl=%s", rate, hits, total, htnApi.gbtCacheTTL)
		        }
			return cached, nil
		}
		htnApi.gbtCacheMu.Unlock()
		atomic.AddUint64(&htnApi.gbtCacheMisses, 1)

		// Cache miss or expired – fetch outside the lock.
		template, err := htnApi.hoosat.GetBlockTemplate(payoutAddress, extraData)
		if err != nil {
			return nil, errors.Wrap(err, "failed fetching new block template from hoosat")
		}
		htnApi.gbtCacheMu.Lock()
		htnApi.gbtCache[payoutAddress] = &gbtCacheEntry{template: template, fetchedAt: time.Now()}
		htnApi.gbtCacheMu.Unlock()
		return template, nil
	}

	// Cache disabled or poll/vote path.
	template, err := htnApi.hoosat.GetBlockTemplate(payoutAddress, extraData)
	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from hoosat")
	}
	return template, nil
}
