package gostratum

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/util"
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
	logger := testLogger()
	listener := NewListener(DefaultConfig(logger))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	mc := NewMockConnection()
	listener.newClient(ctx, mc)
	// send in the authorize event
	event, _ := json.Marshal(NewEvent("1", "mining.authorize", []any{
		"", "test",
	}))
	mc.AsyncWriteTestDataToReadBuffer(string(event))

	responseReceived := false
	mc.ReadTestDataFromBuffer(func(b []byte) {
		expected := JsonRpcResponse{
			Id:     "1",
			Error:  nil,
			Result: true,
		}
		decoded := JsonRpcResponse{}
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatal(err)
		}
		if d := cmp.Diff(&expected, &decoded); d != "" {
			t.Fatalf("response incorrect: %s", d)
		}
		// done
		responseReceived = true
	})

	if !responseReceived {
		t.Fatalf("failed to properly respond to authorize")
	}
}

func TestWalletValidation(t *testing.T) {
	// Real addresses: the earlier literals were Kaspa addresses with the prefix swapped, so their checksums
	// failed and the test could not pass.
	addr := func(prefix util.Bech32Prefix, b byte) string {
		a, err := util.NewAddressPublicKey(bytes.Repeat([]byte{b}, 32), prefix)
		if err != nil {
			t.Fatal(err)
		}
		return a.String()
	}
	main, test := addr(util.Bech32PrefixHoosat, 1), addr(util.Bech32PrefixHoosatTest, 2)
	broken := main[:len(main)-1] + "q" // change the last checksum character
	if main[len(main)-1] == 'q' {
		broken = main[:len(main)-1] + "p"
	}
	tests := []struct {
		in        string
		expected  string
		shouldErr bool
	}{
		{in: main, expected: main},
		{in: test, expected: test},
		{in: main + ",Rig_3784816", expected: main},
		{in: test + ",Rig_3784816", expected: test},
		{in: strings.TrimPrefix(main, "hoosat:") + ",Rig_3784816", expected: main},
		{in: broken + ",Rig_3784816", shouldErr: true},     // checksum broken: no valid address to keep
		{in: "x" + main + ",Rig_3784816", shouldErr: true}, // the address must start the login
		{in: "", shouldErr: true},
		// anything outside [a-z0-9] ends the address and is discarded
		{in: main + ",", expected: main},
		{in: main + " rig1", expected: main},
		{in: main + "RIG1", expected: main},
		// a lowercase extension is part of the candidate and must decode in full
		{in: main + "x", shouldErr: true},
		{in: main + "x,rig1", shouldErr: true},
	}

	for _, v := range tests {
		cleaned, err := CleanWallet(v.in)
		if err != nil && !v.shouldErr {
			t.Fatalf("Unexpected error for wallet %+v: %v", v, err)
		}
		if err == nil && v.shouldErr {
			t.Fatalf("wallet %+v accepted as %s", v, cleaned)
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
