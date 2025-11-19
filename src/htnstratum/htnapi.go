package htnstratum

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hoosat-Oy/HTND/app/appmessage"
	"github.com/Hoosat-Oy/HTND/infrastructure/network/rpcclient"
	"github.com/Hoosat-Oy/htn-stratum-bridge/src/bridgefee"
	"github.com/Hoosat-Oy/htn-stratum-bridge/src/gostratum"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type HtnApi struct {
	address       string
	blockWaitTime time.Duration
	logger        *zap.SugaredLogger
	hoosat        *rpcclient.RPCClient
	connected     bool
	bridgeFee     BridgeFeeConfig
	jobCounter    uint64 // Atomic counter for unique job identifiers
}

func NewHoosatAPI(address string, blockWaitTime time.Duration, logger *zap.SugaredLogger, bridgeFee BridgeFeeConfig) (*HtnApi, error) {
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
		bridgeFee:     bridgeFee,
		jobCounter:    0,
	}, nil
}

func (htnApi *HtnApi) Start(ctx context.Context, cfg BridgeConfig, blockCb func()) {
	if !cfg.MineWhenNotSynced {
		htnApi.waitForSync(true)
	}
	go htnApi.startBlockTemplateListener(ctx, blockCb)
	go htnApi.startStatsThread(ctx)
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

func (htnApi *HtnApi) reconnect() error {
	if htnApi.hoosat != nil {
		return htnApi.hoosat.Reconnect()
	}

	client, err := rpcclient.NewRPCClient(htnApi.address)
	if err != nil {
		return err
	}
	htnApi.hoosat = client
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
				time.Sleep(30 * time.Second)
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
	// Determine the target payout address (miner or bridge)
	payoutAddress := client.WalletAddr

	// Check if bridge fee is enabled and should replace this GBT
	if htnApi.bridgeFee.Enabled && htnApi.bridgeFee.ServerSalt != "" {
		// Get the latest block DAG info to retrieve prevBlockHash
		dagInfo, err := htnApi.hoosat.GetBlockDAGInfo()
		if err != nil {
			htnApi.logger.Warn("failed to get DAG info for bridge fee calculation", zap.Error(err))
		} else if len(dagInfo.TipHashes) > 0 {
			// Increment global job counter atomically
			jobCounterValue := atomic.AddUint64(&htnApi.jobCounter, 1)

			// Decode prevBlockHash from hex
			prevBlockHashBytes, err := hex.DecodeString(dagInfo.TipHashes[0])
			if err != nil {
				// If decode fails, log and skip bridge fee selection for this GBT
				htnApi.logger.Warn("failed to decode prevBlockHash for bridge fee calculation, skipping selection",
					zap.Error(err),
					zap.String("prev_block_hash_hex", dagInfo.TipHashes[0]))
			} else {
				// Build jobKey: jobCounter || prevBlockHash || timestamp || workerID
				workerID := []byte(sanitizeWorkerID(client.WorkerName))
				jobKeyLen := 8 + len(prevBlockHashBytes) + 8 + len(workerID)
				jobKey := make([]byte, jobKeyLen)
				
				// Add job counter (8 bytes, big-endian)
				offset := 0
				binary.BigEndian.PutUint64(jobKey[offset:], jobCounterValue)
				offset += 8
				
				// Add prevBlockHash bytes
				copy(jobKey[offset:], prevBlockHashBytes)
				offset += len(prevBlockHashBytes)
				
				// Add timestamp (8 bytes, big-endian)
				binary.BigEndian.PutUint64(jobKey[offset:], uint64(time.Now().Unix()))
				offset += 8
				
				// Add workerID (UTF-8 bytes)
				copy(jobKey[offset:], workerID)

				// Check if this GBT should be diverted to bridge address
				if bridgefee.ShouldReplaceGBT(htnApi.bridgeFee.ServerSalt, htnApi.bridgeFee.RatePpm, jobKey) {
					payoutAddress = htnApi.bridgeFee.Address
					htnApi.logger.Info("diverting GBT to bridge address",
						zap.Uint64("job_counter", jobCounterValue),
						zap.String("prev_block_hash", dagInfo.TipHashes[0]),
						zap.String("worker", sanitizeWorkerID(client.WorkerName)))
					RecordDivertedGBT()
				}
			}
		}
	}

	// Build extraData string
	var extraData string
	if poll != 0 && vote != 0 {
		extraData = fmt.Sprintf(`'%s' via htn-stratum-bridge_%s as worker %s poll %d vote %d`, 
			client.RemoteApp, version, sanitizeWorkerID(client.WorkerName), poll, vote)
	} else {
		extraData = fmt.Sprintf(`'%s' via htn-stratum-bridge_%s as worker %s`, 
			client.RemoteApp, version, sanitizeWorkerID(client.WorkerName))
	}

	// Request block template with selected payout address
	template, err := htnApi.hoosat.GetBlockTemplate(payoutAddress, extraData)
	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from hoosat")
	}

	return template, nil
}
