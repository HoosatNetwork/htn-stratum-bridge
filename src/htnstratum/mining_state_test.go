package htnstratum

import (
	"sync"
	"testing"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
)

// A job whose ring slot has been reused must not resolve to the newer job's block.
func TestMiningStateJobSlotReuse(t *testing.T) {
	ms := MiningStateGenerator().(*MiningState)
	first := &appmessage.RPCBlock{}
	firstID := ms.AddJob(first)
	for i := 0; i < maxjobs-1; i++ {
		ms.AddJob(&appmessage.RPCBlock{})
	}
	if job, ok := ms.GetJob(firstID); !ok || job != first {
		t.Fatalf("job %d should still be available after %d newer jobs", firstID, maxjobs-1)
	}

	reused := &appmessage.RPCBlock{}
	reusedID := ms.AddJob(reused) // same slot as firstID
	if _, ok := ms.GetJob(firstID); ok {
		t.Fatalf("job %d resolved after its slot was reused by job %d", firstID, reusedID)
	}
	ms.RemoveJob(firstID) // must not delete the newer job sharing the slot
	if job, ok := ms.GetJob(reusedID); !ok || job != reused {
		t.Fatalf("RemoveJob(%d) removed newer job %d", firstID, reusedID)
	}
}

// Concurrent job updates for one client must get distinct ids, each resolving to its own block.
func TestMiningStateConcurrentAddJob(t *testing.T) {
	ms := MiningStateGenerator().(*MiningState)
	const n = maxjobs // every job stays resolvable
	type added struct {
		id    int
		block *appmessage.RPCBlock
	}
	results := make(chan added, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := &appmessage.RPCBlock{}
			results <- added{id: ms.AddJob(b), block: b}
		}()
	}
	wg.Wait()
	close(results)

	seen := map[int]bool{}
	for r := range results {
		if seen[r.id] {
			t.Fatalf("job id %d handed out twice", r.id)
		}
		seen[r.id] = true
		if job, ok := ms.GetJob(r.id); !ok || job != r.block {
			t.Fatalf("job %d does not resolve to the block it was added with", r.id)
		}
	}
}
