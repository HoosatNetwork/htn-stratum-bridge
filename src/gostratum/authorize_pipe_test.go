package gostratum

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// pipeClient is one client on a real in-memory connection: the test is the miner on peer.
type pipeClient struct {
	listener *StratumListener
	peer     net.Conn
	reader   *bufio.Reader
	cancel   context.CancelFunc
}

func newPipeClient(t *testing.T) *pipeClient {
	t.Helper()
	listener := NewListener(DefaultConfig(testLogger()))
	// One buffered slot: the client's disconnect send never blocks, even when the test fails before draining it.
	listener.disconnectChannel = make(DisconnectChannel, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	peer, server := net.Pipe()
	listener.newClient(ctx, server)
	if err := peer.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	c := &pipeClient{listener: listener, peer: peer, reader: bufio.NewReader(peer), cancel: cancel}
	t.Cleanup(func() { peer.Close(); cancel() })
	return c
}

// send writes one line; net.Pipe is synchronous, so it runs beside the reads.
func (c *pipeClient) send(line []byte) {
	go c.peer.Write(append(line, '\n'))
}

func (c *pipeClient) readLine(t *testing.T) []byte {
	t.Helper()
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("reading reply: %v", err)
	}
	return line
}

// requireClosed fails unless the bridge closes the connection with no further frame.
func (c *pipeClient) requireClosed(t *testing.T) {
	t.Helper()
	if extra, err := c.reader.ReadBytes('\n'); err != io.EOF || len(extra) != 0 {
		t.Fatalf("expected the connection to close after one reply, got %q, %v", extra, err)
	}
}

// requireDisconnect waits for the listener's disconnect event for this client.
func (c *pipeClient) requireDisconnect(t *testing.T) {
	t.Helper()
	select {
	case <-c.listener.disconnectChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("no disconnect event")
	}
}
