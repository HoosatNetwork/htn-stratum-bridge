package htnstratum

import (
	"math/big"
	"sync"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
	"github.com/HoosatNetwork/htn-stratum-bridge/src/gostratum"
)

const maxjobs = 32

// jobEntry remembers which job id owns a ring slot, so a lookup for an id whose slot has
// since been reused is reported as a missing (stale) job instead of returning another job's block.
type jobEntry struct {
	id    int
	block *appmessage.RPCBlock
}

type MiningState struct {
	Jobs        map[int]jobEntry
	JobLock     sync.Mutex
	jobCounter  int
	bigDiff     big.Int
	initialized bool
	useBigJob   bool
	connectTime time.Time
	stratumDiff *hoosatDiff
}

func MiningStateGenerator() any {
	return &MiningState{
		Jobs:        map[int]jobEntry{},
		JobLock:     sync.Mutex{},
		connectTime: time.Now(),
	}
}

func GetMiningState(ctx *gostratum.StratumContext) *MiningState {
	return ctx.State.(*MiningState)
}

func (ms *MiningState) AddJob(job *appmessage.RPCBlock) int {
	// allocate the id under the lock: concurrent job updates for one client must not share an id
	ms.JobLock.Lock()
	ms.jobCounter++
	idx := ms.jobCounter
	ms.Jobs[idx%maxjobs] = jobEntry{id: idx, block: job}
	ms.JobLock.Unlock()
	return idx
}

func (ms *MiningState) GetJob(id int) (*appmessage.RPCBlock, bool) {
	ms.JobLock.Lock()
	entry, exists := ms.Jobs[id%maxjobs]
	ms.JobLock.Unlock()
	if !exists || entry.id != id {
		return nil, false
	}
	return entry.block, true
}

func (ms *MiningState) RemoveJob(id int) {
	ms.JobLock.Lock()
	// only drop the slot if it still holds this job, not a newer one that reused it
	if entry, ok := ms.Jobs[id%maxjobs]; ok && entry.id == id {
		delete(ms.Jobs, id%maxjobs)
	}
	ms.JobLock.Unlock()
}

func (ms *MiningState) ClearJobs() {
	ms.JobLock.Lock()
	ms.Jobs = make(map[int]jobEntry)
	ms.JobLock.Unlock()
}
