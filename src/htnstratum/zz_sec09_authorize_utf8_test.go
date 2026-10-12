package htnstratum

import (
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/HoosatNetwork/htn-stratum-bridge/src/gostratum"
	"go.uber.org/zap"
)

// A single mining.authorize whose address has a 2-byte rune straddling byte 61
// used to crash the whole bridge: CleanWallet cut the address at byte 61,
// splitting the rune, so ctx.WalletAddr held invalid UTF-8. getCreateStats then
// passed it to a prometheus label, and CounterVec.With panics on invalid UTF-8.
// That panic ran on the per-connection goroutine, which had no recover, so the
// process exited and every miner was dropped.
//
// Any TCP client reaching the stratum port can send this; it needs no prior
// subscribe and no valid address.
func TestAuthorizeSplitRuneDoesNotPanic(t *testing.T) {
	sh := newShareHandler(nil, false, func() {}, false)
	ctx := newSwallowingContext09()

	ev, err := gostratum.UnmarshalEvent(
		`{"id":1,"method":"mining.authorize","params":["hoosat:` + strings.Repeat("a", 53) + `é"]}`)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("authorize panicked, the bridge process would exit: %v", r)
		}
	}()

	if err := gostratum.HandleAuthorize(ctx, ev); err != nil {
		return // address rejected before it can reach a label: the fix
	}
	sh.getCreateStats(ctx) // what ListenAndServe's authorize wrapper does next
}

// discardConn09 is a net.Conn that swallows every write without blocking, so
// HandleAuthorize's reply succeeds and the test reaches getCreateStats.
type discardConn09 struct{}

func (discardConn09) Read(b []byte) (int, error)         { return 0, net.ErrClosed }
func (discardConn09) Write(b []byte) (int, error)        { return len(b), nil }
func (discardConn09) Close() error                       { return nil }
func (discardConn09) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (discardConn09) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (discardConn09) SetDeadline(t time.Time) error      { return nil }
func (discardConn09) SetReadDeadline(t time.Time) error  { return nil }
func (discardConn09) SetWriteDeadline(t time.Time) error { return nil }

func newSwallowingContext09() *gostratum.StratumContext {
	ctx := &gostratum.StratumContext{Logger: zap.NewNop(), RemoteAddr: "127.0.0.1"}
	setUnexported09(ctx, "parentContext", context.Background())
	setUnexported09(ctx, "connection", discardConn09{})
	return ctx
}

func setUnexported09(ctx *gostratum.StratumContext, field string, v any) {
	f := reflect.ValueOf(ctx).Elem().FieldByName(field)
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(v))
}
