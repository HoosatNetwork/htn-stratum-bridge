package htnstratum

import (
	"testing"

	"github.com/HoosatNetwork/HTND/app/appmessage"
	"github.com/HoosatNetwork/htn-stratum-bridge/src/gostratum"
)

func TestPromValid(t *testing.T) {
	// mismatched prom labels throw a panic, sanity check that everything
	// is valid to write to here
	ctx := gostratum.StratumContext{}

	RecordShareFound(&ctx, 300000)
	RecordStaleShare(&ctx)
	RecordDupeShare(&ctx)
	RecordInvalidShare(&ctx)
	RecordWeakShare(&ctx)
	RecordBlockFound(&ctx, 10000, 12345, "abcdefg")
	RecordDisconnect(&ctx)
	RecordNewJob(&ctx)
	RecordNetworkStats(1234, 5678, 910)
	RecordWorkerError("localhost", ErrDisconnected)
	RecordBalances(&appmessage.GetBalancesByAddressesResponseMessage{
		Entries: []*appmessage.BalancesByAddressesEntry{
			{
				Address: "localhost",
				Balance: 1234,
			},
		},
	})
}
