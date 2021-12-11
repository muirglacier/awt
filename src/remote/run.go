package remote

import (
	"awt/test"
	"bufio"
	"context"
	"crypto/ecdsa"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/muirglacier/aw/dht/dhtutil"
	"github.com/muirglacier/aw/peer"
	"github.com/muirglacier/aw/wire"
	"github.com/muirglacier/id"
	"math/big"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func Run(index int, topology test.Topology, outputFilename string, correctness bool, perf bool, testOptions test.Options) {
	ipFile, err := os.Open("../ip")
	if err != nil {
		panic(err)
	}
	var ipList []string
	scanner := bufio.NewScanner(ipFile)
	for scanner.Scan() {
		ipAddr := strings.TrimSpace(scanner.Text())
		ipList = append(ipList, ipAddr)
		println("Adding IP address", ipAddr)
	}
	ipFile.Close()

	keyFile, err := os.Open("../keys")
	if err != nil {
		panic(err)
	}

	var key *id.PrivKey
	var sigs []id.Signatory
	keyScanner := bufio.NewScanner(keyFile)
	counter := 0
	for keyScanner.Scan() {
		keyData := strings.SplitN(strings.TrimSpace(keyScanner.Text()), ",", 3)

		x, ok := new(big.Int).SetString(keyData[1], 10)
		if !ok {
			panic("X conversion failed")
		}
		y, ok := new(big.Int).SetString(keyData[2], 10)
		if !ok {
			panic("Y conversion failed")
		}
		pubkey := id.PubKey{
			Curve: crypto.S256(),
			X:     x,
			Y:     y,
		}
		sigs = append(sigs, id.NewSignatory(&pubkey))

		d, ok := new(big.Int).SetString(keyData[0], 10)
		if !ok {
			panic("D conversion failed")
		}
		if counter == index {
			key = &id.PrivKey{
				PublicKey: ecdsa.PublicKey(pubkey),
				D: d,
			}
		}

		counter++
	}

	if len(sigs) != len(ipList) {
		panic(fmt.Sprintf("length of sigs and ip(s) don't match: %v != %v", len(sigs), len(ipList)))
	}

	GossipPerf(index, key, sigs, ipList, test.Topology(topology), outputFilename, correctness, perf, testOptions)
}

func GossipPerf(index int, key *id.PrivKey, sigs []id.Signatory, ipList []string, topology test.Topology, outputFilename string, correctness bool, perf bool, testOptions test.Options) {
	pwd, _ := os.Getwd()
	fo, err := os.Create(pwd + "/../output.txt")
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		fo.Sync()
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	_, p, table, contentResolver, _, t := setup(key, testOptions)

	// Add Topology logic (currently only ues ring)
	if index == 0 {
		p.Link(sigs[len(sigs)-1])
		table.AddPeer(sigs[len(sigs)-1],
			wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", ipList[len(sigs)-1], 8080),
				uint64(time.Now().UnixNano())))
	} else {
		p.Link(sigs[index-1])
		table.AddPeer(sigs[index-1],
			wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", ipList[index-1], 8080),
				uint64(time.Now().UnixNano())))
	}

	p.Link(sigs[(index+1) % len(sigs)])
	table.AddPeer(sigs[(index+1) % len(sigs)],
		wire.NewUnsignedAddress(wire.TCP,
			fmt.Sprintf("%v:%v", ipList[(index+1) % len(sigs)], 8080),
			uint64(time.Now().UnixNano())))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()


	go func() {
		t.Receive(context.Background(), func() func(from id.Signatory, packet wire.Packet) error {
			var x int64 = 0
			go func() {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						_, err = fo.WriteString(fmt.Sprintf("%d\n", x))
						atomic.StoreInt64(&x, 0)
						if err != nil {
							fmt.Printf("error writing to file: %v", err)
						}
					}
				}
			}()
			return func(from id.Signatory, packet wire.Packet) error {
				atomic.AddInt64(&x, 1)
				if err := p.Syncer().DidReceiveMessage(from, packet.Msg); err != nil {
					return err
				}
				if err := p.Gossiper().DidReceiveMessage(from, packet.Msg); err != nil {
					return err
				}
				return nil
			}
		}())
		t.Run(ctx)
	}()

	time.Sleep(5*time.Second)
	println("Node up and running")

	for iter := 0; iter < 50000; iter++ {
		ctxGossip, cancelGossip := context.WithTimeout(context.Background(), time.Second*2)
		msgHello := fmt.Sprintf(string(dhtutil.RandomContent()), p.ID().String())
		contentID := id.NewHash([]byte(msgHello))
		contentResolver.InsertContent(contentID[:], []byte(msgHello))
		p.Gossip(ctxGossip, contentID[:], &peer.DefaultSubnet)
		cancelGossip()
	}
	time.Sleep(10*time.Second)
}
