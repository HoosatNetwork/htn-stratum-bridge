package gostratum

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
)

// startTestClient runs spawnClientListener on one end of a pipe and returns the other end.
// Every mining.subscribe event's id is sent to the returned channel.
func startTestClient(t *testing.T) (net.Conn, *StratumContext, chan any) {
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close() })
	ids := make(chan any, 16)
	s := &StratumListener{StratumListenerConfig: StratumListenerConfig{
		HandlerMap: StratumHandlerMap{"mining.subscribe": func(_ *StratumContext, e JsonRpcEvent) error {
			ids <- e.Id
			return nil
		}},
	}}
	ctx := &StratumContext{parentContext: context.Background(), Logger: zap.NewNop(), connection: server}
	go spawnClientListener(ctx, server, s)
	return client, ctx, ids
}

// testLine returns a valid JSON-RPC line of exactly total bytes including term,
// or a short line when total is 0.
func testLine(id string, total int, term string) string {
	prefix := `{"id":"` + id + `","method":"mining.subscribe","params":["`
	suffix := `"]}` + term
	pad := 1
	if total > 0 {
		pad = total - len(prefix) - len(suffix)
	}
	return prefix + strings.Repeat("a", pad) + suffix
}

func TestReadFromConnectionFraming(t *testing.T) {
	a, b := testLine("a", 0, "\n"), testLine("b", 0, "\n")
	aCRLF := testLine("a", 0, "\r\n")
	exact := testLine("a", maxLineLength, "\n")
	exactCRLF := testLine("a", maxLineLength, "\r\n")
	over := testLine("a", maxLineLength+1, "\n")
	overCRLF := testLine("a", maxLineLength+1, "\r\n")
	big := testLine("a", 20000, "\n")

	tests := []struct {
		name           string
		writes         []string
		eof            bool
		want           []any
		wantDisconnect bool
	}{
		{name: "split across reads", writes: []string{a[:20], a[20:]}, want: []any{"a"}},
		{name: "CRLF", writes: []string{aCRLF}, want: []any{"a"}},
		{name: "split between CR and LF", writes: []string{aCRLF[:len(aCRLF)-1], "\n"}, want: []any{"a"}},
		{name: "two lines in one read", writes: []string{a + b}, want: []any{"a", "b"}},
		{name: "split right after LF", writes: []string{a, b}, want: []any{"a", "b"}},
		{name: "complete line then partial line", writes: []string{a + b[:10], b[10:]}, want: []any{"a", "b"}},
		{name: "NUL bytes stripped", writes: []string{a[:1] + "\x00" + a[1:]}, want: []any{"a"}},
		{name: "exact limit", writes: []string{exact}, want: []any{"a"}},
		{name: "exact limit split", writes: []string{exact[:10000], exact[10000:]}, want: []any{"a"}},
		{name: "exact limit CRLF", writes: []string{exactCRLF}, want: []any{"a"}},
		{name: "limit plus one", writes: []string{over}, wantDisconnect: true},
		{name: "limit plus one CRLF", writes: []string{overCRLF}, wantDisconnect: true},
		{name: "oversized line split 10000+10000", writes: []string{big[:10000], big[10000:]}, wantDisconnect: true},
		{name: "oversized line split 16192+3808", writes: []string{big[:16192], big[16192:]}, wantDisconnect: true},
		{name: "EOF after partial JSON", writes: []string{a[:20]}, eof: true, wantDisconnect: true},
		{name: "EOF after JSON without newline", writes: []string{a[:len(a)-1]}, eof: true, wantDisconnect: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, ctx, ids := startTestClient(t)
			for _, w := range tt.writes {
				client.Write([]byte(w)) // fails once the bridge has closed the connection
			}
			if tt.eof {
				client.Close()
			}
			var got []any
			deadline := time.After(2 * time.Second)
			for len(got) < len(tt.want) || (tt.wantDisconnect && !ctx.disconnecting.Load()) {
				select {
				case id := <-ids:
					got = append(got, id)
				case <-deadline:
					t.Fatalf("timed out: got %v, disconnected %v", got, ctx.disconnecting.Load())
				case <-time.After(10 * time.Millisecond):
				}
			}
			time.Sleep(20 * time.Millisecond) // let any extra event or disconnect show up
			for len(ids) > 0 {
				got = append(got, <-ids)
			}
			if d := cmp.Diff(tt.want, got); d != "" {
				t.Errorf("handled ids (-want +got): %s", d)
			}
			if got := ctx.disconnecting.Load(); got != tt.wantDisconnect {
				t.Errorf("disconnected = %v, want %v", got, tt.wantDisconnect)
			}
		})
	}
}

// A line is not handled before its newline arrives.
func TestLineWaitsForNewline(t *testing.T) {
	client, _, ids := startTestClient(t)
	line := testLine("a", 0, "\n")
	client.Write([]byte(line[:len(line)-1]))
	select {
	case id := <-ids:
		t.Fatalf("line %v handled before its newline", id)
	case <-time.After(50 * time.Millisecond):
	}
	client.Write([]byte("\n"))
	select {
	case <-ids:
	case <-time.After(2 * time.Second):
		t.Fatal("line not handled after its newline")
	}
}
