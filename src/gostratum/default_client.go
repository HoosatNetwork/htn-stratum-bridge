package gostratum

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/HoosatNetwork/HTND/v2/util"
	"github.com/mattn/go-colorable"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type StratumMethod string

const (
	StratumMethodSubscribe StratumMethod = "mining.subscribe"
	StratumMethodAuthorize StratumMethod = "mining.authorize"
	StratumMethodSubmit    StratumMethod = "mining.submit"
)

func DefaultLogger() *zap.Logger {
	cfg := zap.NewDevelopmentEncoderConfig()
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.AddSync(colorable.NewColorableStdout()),
		zapcore.DebugLevel,
	))
}

func DefaultConfig(logger *zap.Logger) StratumListenerConfig {
	return StratumListenerConfig{
		StateGenerator: func() any { return nil },
		HandlerMap:     DefaultHandlers(),
		Port:           ":5555",
		Logger:         logger,
	}
}

func DefaultHandlers() StratumHandlerMap {
	return StratumHandlerMap{
		string(StratumMethodSubscribe): HandleSubscribe,
		string(StratumMethodAuthorize): HandleAuthorize,
		string(StratumMethodSubmit):    HandleSubmit,
	}
}

// Bounds on what one miner connection can make the bridge retain: every
// authorize with a new worker name creates per-worker stats and prometheus
// series that are kept after the connection closes. maxAuthorizePerConn is a
// guess at how many rigs a proxy multiplexes on one connection; adjust to taste.
const (
	maxAuthorizePerConn = 4
	maxWorkerNameLen    = 64
)

func HandleAuthorize(ctx *StratumContext, event JsonRpcEvent) error {
	ctx.authorizeCount++
	if ctx.authorizeCount > maxAuthorizePerConn {
		return fmt.Errorf("too many authorize requests on one connection")
	}
	if len(event.Params) < 1 {
		return fmt.Errorf("malformed event from miner, expected param[1] to be address")
	}
	address, ok := event.Params[0].(string)
	if !ok {
		return fmt.Errorf("malformed event from miner, expected param[1] to be address string")
	}
	parts := strings.Split(address, ".")
	var workerName string
	if len(parts) >= 2 {
		address = parts[0]
		workerName = parts[1]
	}
	if len(workerName) > maxWorkerNameLen {
		workerName = strings.ToValidUTF8(workerName[:maxWorkerNameLen], "")
	}
	var err error
	address, err = CleanWallet(address)
	if err != nil {
		return fmt.Errorf("invalid wallet format %s: %w", address, err)
	}

	ctx.WalletAddr = address
	ctx.WorkerName = workerName
	ctx.Logger = ctx.Logger.With(zap.String("worker", ctx.WorkerName), zap.String("addr", ctx.WalletAddr))

	if err := ctx.Reply(NewResponse(event, true, nil)); err != nil {
		return errors.Wrap(err, "failed to send response to authorize")
	}
	if ctx.Extranonce != "" {
		SendExtranonce(ctx)
	}

	ctx.Logger.Debug(fmt.Sprintf("client authorized, address: %s", ctx.WalletAddr))
	return nil
}

func HandleSubscribe(ctx *StratumContext, event JsonRpcEvent) error {
	if err := ctx.Reply(NewResponse(event,
		[]any{true, "EthereumStratum/1.0.0"}, nil)); err != nil {
		return errors.Wrap(err, "failed to send response to subscribe")
	}
	if len(event.Params) > 0 {
		app, ok := event.Params[0].(string)
		if ok {
			ctx.RemoteApp = app
		}
	}

	ctx.Logger.Debug("client subscribed ", zap.Any("context", ctx))
	return nil
}

func HandleSubmit(ctx *StratumContext, event JsonRpcEvent) error {
	// stub
	ctx.Logger.Debug("work submission")
	return nil
}

func SendExtranonce(ctx *StratumContext) {
	if err := ctx.Send(NewEvent("", "set_extranonce", []any{ctx.Extranonce})); err != nil {
		// should we doing anything further on failure
		ctx.Logger.Error(errors.Wrap(err, "failed to set extranonce").Error(), zap.Any("context", ctx))
	}
}

// walletRegex matches the address at the start of a login; CleanWallet discards what follows it.
var walletRegex = regexp.MustCompile("^(hoosat|hoosattest):[a-z0-9]+")

func CleanWallet(in string) (string, error) {
	// Check if the input has a valid address for either prefix
	_, err := util.DecodeAddress(in, util.Bech32PrefixHoosat)
	if err == nil {
		return in, nil // valid hoosat address
	}
	_, err = util.DecodeAddress(in, util.Bech32PrefixHoosatTest)
	if err == nil {
		return in, nil // valid hoosattest address
	}

	// Add prefix if it's missing, default to "hoosat:"
	if !strings.HasPrefix(in, "hoosat:") && !strings.HasPrefix(in, "hoosattest:") {
		return CleanWallet("hoosat:" + in)
	}

	// Take the address at the start and discard everything after it: any byte outside [a-z0-9] ends the
	// address, so "<addr>,rig1", "<addr> rig1" or "<addr>," all give <addr> (only "." sets a worker name, in
	// HandleAuthorize). A longer lowercase run must decode in full. Only a valid address is returned: the
	// result reaches a prometheus label value, which panics on invalid UTF-8. (This used to cut at byte 61,
	// which cuts a mainnet Schnorr address short.) Clone so the kept wallet does not pin the whole login.
	if m := walletRegex.FindString(in); m != "" && m != in {
		prefix := util.Bech32PrefixHoosat
		if strings.HasPrefix(m, "hoosattest:") {
			prefix = util.Bech32PrefixHoosatTest
		}
		if _, err := util.DecodeAddress(m, prefix); err == nil {
			return strings.Clone(m), nil
		}
	}
	return "", errors.New("unable to coerce wallet to valid hoosat or hoosattest address")
}
