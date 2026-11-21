package gostratum

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/mattn/go-colorable"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func testLogger() *zap.Logger {
	cfg := zap.NewDevelopmentEncoderConfig()
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.AddSync(colorable.NewColorableStdout()),
		zapcore.DebugLevel,
	))
}

func TestAcceptContextLifetime(t *testing.T) {
	logger := testLogger()

	listener := NewListener(DefaultConfig(logger))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

	defer cancel()
	listener.Listen(ctx)
}

func TestNewClient(t *testing.T) {
	// A valid wallet is authorized; an invalid one gets one JSON-RPC error (which never echoes the login), then the
	// connection closes. (The test used to expect true for the empty wallet, so it could not pass.)
	refusal := `{"id":"1","result":false,"error":[24,"invalid wallet address",null]}`
	for _, c := range []struct {
		wallet string
		want   string
	}{
		{"hoosat:qqqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqsz6kzgffrt", `{"id":"1","result":true,"error":null}`},
		{"", refusal},
		{"hoosat:notanaddress", refusal},
	} {
		client := newPipeClient(t)
		event, _ := json.Marshal(NewEvent("1", "mining.authorize", []any{c.wallet, "test"}))
		client.send(event)

		line := client.readLine(t)
		expected, decoded := JsonRpcResponse{}, JsonRpcResponse{}
		if err := json.Unmarshal([]byte(c.want), &expected); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(line, &decoded); err != nil {
			t.Fatalf("wallet %q: %v", c.wallet, err)
		}
		if d := cmp.Diff(&expected, &decoded); d != "" {
			t.Fatalf("wallet %q: response incorrect: %s", c.wallet, d)
		}
		if c.want == refusal {
			client.requireClosed(t)
		} else {
			client.peer.Close()
		}
		client.requireDisconnect(t)
	}
}

func TestWalletValidation(t *testing.T) {
	tests := []struct {
		in        string
		expected  string
		shouldErr bool
	}{
		{
			in:       "hoosat:qqayxgcjfh6d7uxpj4w3qzjvx73vdehfx22fl6cacmn44rpj5geg2rxyuhga4,Rig_3784816",
			expected: "hoosat:qqayxgcjfh6d7uxpj4w3qzjvx73vdehfx22fl6cacmn44rpj5geg2rxyuhga4",
		},
		{
			in:       "hoosat:qqkrl0er5ka5snd55gr9rcf6rlpx8nln8gf3jxf83w4dc0khfqmauy6qs83zm,Rig_3784816",
			expected: "hoosat:qqkrl0er5ka5snd55gr9rcf6rlpx8nln8gf3jxf83w4dc0khfqmauy6qs83zm",
		},
		{
			in:       "qqkrl0er5ka5snd55gr9rcf6rlpx8nln8gf3jxf83w4dc0khfqmauy6qs83zm,Rig_3784816",
			expected: "hoosat:qqkrl0er5ka5snd55gr9rcf6rlpx8nln8gf3jxf83w4dc0khfqmauy6qs83zm",
		},
	}

	for _, v := range tests {
		cleaned, err := CleanWallet(v.in)
		if err != nil && !v.shouldErr {
			t.Fatalf("Unexpected error for wallet %+v", v)
		}
		if cleaned != v.expected {
			t.Fatalf("expected %s, got %s", v.expected, cleaned)
		}
	}
}

func TestSendReturnsDisconnectedAfterDisconnect(t *testing.T) {
	logger := testLogger()
	ctx, _ := NewMockContext(context.Background(), logger, nil)

	ctx.Disconnect()

	err := ctx.Send(JsonRpcEvent{
		Version: "2.0",
		Method:  "mining.set_difficulty",
		Params:  []any{1.0},
	})
	if !errors.Is(err, ErrorDisconnected) {
		t.Fatalf("expected ErrorDisconnected, got %v", err)
	}
	if ctx.Connected() {
		t.Fatal("expected disconnected context")
	}
}

func TestDisconnectIsIdempotent(t *testing.T) {
	ctx := &StratumContext{
		Logger:       testLogger(),
		onDisconnect: make(chan *StratumContext, 2),
	}

	ctx.Disconnect()
	ctx.Disconnect()

	if got := len(ctx.onDisconnect); got != 1 {
		t.Fatalf("expected one disconnect notification, got %d", got)
	}
	if ctx.Connected() {
		t.Fatal("expected disconnected context")
	}
}
