package htnstratum

import (
	"encoding/json"
	"os"

	"log"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/model/externalapi"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/consensushashing"
	"github.com/HoosatNetwork/HTND/v2/util/difficulty"
	"github.com/google/go-cmp/cmp"
)

func TestHeaderSerialization(t *testing.T) {
	raw, err := os.ReadFile("./example_header.json")
	if err != nil {
		t.Fatal(err)
	}
	block := appmessage.RPCBlock{}
	if err := json.Unmarshal(raw, &block.Header); err != nil {
		t.Fatal(err)
	}

	header, err := SerializeBlockHeader(&block)
	if err != nil {
		t.Fatal(err)
	}
	// expected: HTND's own header hash with timestamp and nonce zeroed (the pre-PoW hash)
	domainBlock, err := appmessage.RPCBlockToDomainBlock(&block, new(externalapi.DomainHash).String())
	if err != nil {
		t.Fatal(err)
	}
	prePow := domainBlock.Header.ToMutable()
	prePow.SetTimeInMilliseconds(0)
	prePow.SetNonce(0)
	headerExpected := consensushashing.HeaderHash(prePow.ToImmutable()).ByteSlice()
	if d := cmp.Diff(headerExpected, header); d != "" {
		t.Fatalf("header generated incorrectly: %s", d)
	}

	{ // job separated to parts (lolminer/srbminer)
		jobs := GenerateJobHeader(header)
		expected := []uint64{13010215636699018172, 9778935575291462752, 8775572449963272124, 535084289290003726}
		if d := cmp.Diff(expected, jobs); d != "" {
			t.Fatalf("jobs generated incorrectly: %s", d)
		}
		log.Printf("%+v", jobs)
	}

	{ // job as single string (bzminer)
		job := GenerateLargeJobParams(header, 1662696346)
		expected := "bc73adc430928db46084ad3e18c2b587bce7c3f4a518c9790ee5057c53006d079abb1a6300000000"
		if d := cmp.Diff(expected, job); d != "" {
			t.Fatalf("jobs generated incorrectly: %s", d)
		}
		log.Printf("%+v", job)
	}

	// expected diff: 12617.375671633985 (approx)
	diff := CalculateTarget(453325233)
	little := BigDiffToLittle(&diff)
	if little < 12617 || little > 12618 {
		t.Errorf("wrong difficulty calculated, expected ~12617.375671633985, got %f", little)
	}
}

func TestPoolHzCalculation(t *testing.T) {
	// TODO: figure out what we really want to test here.
	// currently set up diff object to mimic old static settings
	diff := newHoosatDiff()
	diff.setDiffValue(4)
	log.Println(diff.hashValue)
	log.Println(diff.diffValue)
	rate := big.Int{} // 1mhz
	rate.SetUint64(1)
	rate.Lsh(&rate, 222)
	dd := BigDiffToLittle(&rate)
	log.Println(dd)
	//rate.Sub(&rate, big.NewInt(1000000))
	log.Println(difficulty.GetHashrateString(&rate, time.Second*1))
}

// snooper. Inspect coms between miner and pool
func TestBridge(t *testing.T) {
	serverConn, err := net.Dial("tcp", "pool.us.woolypooly.com:3112")
	if err != nil {
		t.Fatal(err)
	}
	clientConn, err := net.Listen("tcp", ":4444")
	if err != nil {
		t.Fatal(err)
	}
	c, _ := clientConn.Accept()
	buff := make([]byte, 1024)
	for {
		time.Sleep(100 * time.Millisecond)
		bytesRead, _ := c.Read(buff)
		log.Printf("client -> server: %d, '%s'", bytesRead, string(buff))
		serverConn.Write(buff)
		bytesRead, _ = serverConn.Read(buff)
		log.Printf("server -> client: %d, '%s'", bytesRead, string(buff))
		c.Write(buff)
	}
}
