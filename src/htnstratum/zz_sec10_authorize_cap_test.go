package htnstratum

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/HoosatNetwork/HTND/v2/util"
	"github.com/HoosatNetwork/htn-stratum-bridge/src/gostratum"
	"go.uber.org/zap"
)

// Repeated mining.authorize on one connection, each with a fresh worker name,
// used to grow per-worker state without bound: each authorize left a WorkStats
// (four maps) and a set of prometheus series keyed by the worker name, kept
// after the connection closed. One connection could drive the bridge's memory
// up on its own.
func TestAuthorizeStateBoundedPerConnection(t *testing.T) {
	addr, err := util.NewAddressPublicKey(make([]byte, 32), util.Bech32PrefixHoosat)
	if err != nil {
		t.Fatal(err)
	}
	sh := newShareHandler(nil, false, func() {}, false)
	ctx := newSwallowingContext10()

	const n = 2000
	for i := 0; i < n; i++ {
		ev := authorizeEvent10(t, addr.String()+"."+strings.Repeat("w", 200)+fmt.Sprint(i))
		if err := gostratum.HandleAuthorize(ctx, ev); err != nil {
			continue // the stratum listener disconnects this connection on error
		}
		sh.getCreateStats(ctx)
	}
	if len(sh.stats) > 8 {
		t.Fatalf("%d authorize msgs on one connection left %d per-worker stats entries", n, len(sh.stats))
	}
}

func authorizeEvent10(t *testing.T, param string) gostratum.JsonRpcEvent {
	ev, err := gostratum.UnmarshalEvent(`{"id":1,"method":"mining.authorize","params":["` + param + `"]}`)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

type discardConn10 struct{}

func (discardConn10) Read(b []byte) (int, error)         { return 0, net.ErrClosed }
func (discardConn10) Write(b []byte) (int, error)        { return len(b), nil }
func (discardConn10) Close() error                       { return nil }
func (discardConn10) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (discardConn10) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (discardConn10) SetDeadline(t time.Time) error      { return nil }
func (discardConn10) SetReadDeadline(t time.Time) error  { return nil }
func (discardConn10) SetWriteDeadline(t time.Time) error { return nil }

func newSwallowingContext10() *gostratum.StratumContext {
	ctx := &gostratum.StratumContext{Logger: zap.NewNop(), RemoteAddr: "127.0.0.1"}
	setUnexported10(ctx, "parentContext", context.Background())
	setUnexported10(ctx, "connection", discardConn10{})
	return ctx
}

func setUnexported10(ctx *gostratum.StratumContext, field string, v any) {
	f := reflect.ValueOf(ctx).Elem().FieldByName(field)
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(v))
}
