package htnstratum

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
	"go.uber.org/zap"
)

// restartingNode is a synced node that announces templates to the handlers
// registered since its last restart. A restart drops them, as a node restart
// ends the RPC client's notification goroutine. The first failures
// registrations fail.
type restartingNode struct {
	mu            sync.Mutex
	handlers      []func(*appmessage.NewBlockTemplateNotificationMessage)
	registrations int
	failures      int
}

func (n *restartingNode) GetInfo() (*appmessage.GetInfoResponseMessage, error) {
	return &appmessage.GetInfoResponseMessage{IsSynced: true}, nil
}

func (n *restartingNode) RegisterForNewBlockTemplateNotifications(h func(*appmessage.NewBlockTemplateNotificationMessage)) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.registrations++
	if n.failures > 0 {
		n.failures--
		return errors.New("registration failed")
	}
	n.handlers = append(n.handlers, h)
	return nil
}

func (n *restartingNode) restart() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers = nil
}

func (n *restartingNode) announce() {
	n.mu.Lock()
	handlers := append([]func(*appmessage.NewBlockTemplateNotificationMessage){}, n.handlers...)
	n.mu.Unlock()
	for _, h := range handlers {
		h(&appmessage.NewBlockTemplateNotificationMessage{})
	}
}

func shortenRenotifyTimers(t *testing.T) {
	s, n, m := syncCheckInterval, notifyStale, maxRenotifyWait
	t.Cleanup(func() { syncCheckInterval, notifyStale, maxRenotifyWait = s, n, m })
	syncCheckInterval, notifyStale, maxRenotifyWait = 10*time.Millisecond, 100*time.Millisecond, time.Second
}

// startListener runs the block template listener against node, with
// announcements every 5 ms and no block_wait_time polling, so jobs only follow
// announcements. It returns a function that waits for the next job.
func startListener(t *testing.T, node *restartingNode) func(when string) {
	shortenRenotifyTimers(t)
	api := newTestHtnApi(0)
	api.blockWaitTime = time.Hour
	api.templateNode = node

	jobs := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		api.startBlockTemplateListener(ctx, false, func() {
			select {
			case jobs <- struct{}{}:
			default:
			}
		})
		close(done)
	}()
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				node.announce()
			}
		}
	}()
	t.Cleanup(func() { close(stop); cancel(); <-done })

	return func(when string) {
		t.Helper()
		select {
		case <-jobs: // a job queued earlier
		default:
		}
		select {
		case <-jobs:
		case <-time.After(2 * time.Second):
			t.Fatalf("no new job %s", when)
		}
	}
}

// After a node restart the listener registers again and new jobs follow the
// node's announcements, without waiting for block_wait_time. Registration
// counts are checked in TestRenotifyIfStale, where they do not depend on
// scheduling.
func TestListenerRegistersAgainAfterNodeRestart(t *testing.T) {
	node := &restartingNode{}
	waitJob := startListener(t, node)
	waitJob("before the restart")

	node.restart()
	time.Sleep(20 * time.Millisecond)
	waitJob("after the restart")
}

// A failed first registration is retried, as after a restart.
func TestListenerRegistersAgainAfterFailedRegistration(t *testing.T) {
	waitJob := startListener(t, &restartingNode{failures: 1})
	waitJob("after the failed registration")
}

// A synced node silent for notifyStale gets a new registration; while it stays
// silent the wait doubles up to maxRenotifyWait, also when registering fails,
// and an announcement after the last registration resets it. A node that
// announces, or is not synced, gets none.
func TestRenotifyIfStale(t *testing.T) {
	api := &HtnApi{logger: zap.NewNop().Sugar()}
	calls := 0
	var registerErr error
	register := func() error { calls++; api.lastRegister.Store(time.Now().UnixNano()); return registerErr }
	ago := func(d time.Duration) int64 { return time.Now().Add(-d).UnixNano() }
	check := func(synced bool, want int, what string) {
		t.Helper()
		api.renotifyIfStale(synced, register)
		if calls != want {
			t.Fatalf("%s: %d registrations, want %d", what, calls, want)
		}
	}

	api.lastNotify.Store(ago(time.Second))
	api.lastRegister.Store(ago(time.Hour))
	check(true, 0, "announcements arrive")

	api.lastNotify.Store(ago(time.Hour))
	check(false, 0, "not synced")
	check(true, 1, "silent synced node")
	check(true, 1, "right after registering")

	api.lastRegister.Store(ago(notifyStale + time.Second))
	check(true, 1, "still silent, wait doubled")
	api.lastRegister.Store(ago(2*notifyStale + time.Second))
	check(true, 2, "still silent after the doubled wait")

	api.lastRegister.Store(ago(notifyStale + 2*time.Second))
	api.lastNotify.Store(ago(notifyStale + time.Second))
	check(true, 3, "silent again after an announcement")

	api.lastNotify.Store(ago(time.Hour))
	registerErr = errors.New("registration failed")
	api.lastRegister.Store(ago(2*notifyStale - time.Second))
	check(true, 3, "silent, wait doubled")
	api.lastRegister.Store(ago(2*notifyStale + time.Second))
	check(true, 4, "registration fails")
	api.lastRegister.Store(ago(2*notifyStale + time.Second))
	check(true, 4, "wait doubled after a failed registration")

	for range 10 {
		api.lastRegister.Store(ago(maxRenotifyWait + time.Second))
		api.renotifyIfStale(true, register)
	}
	if api.renotifyWait != maxRenotifyWait {
		t.Fatalf("wait %v after long silence, want %v", api.renotifyWait, maxRenotifyWait)
	}
}
