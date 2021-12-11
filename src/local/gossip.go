package local

import (
	"awt/test"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/muirglacier/aw/dht/dhtutil"
	"github.com/muirglacier/aw/peer"
	"github.com/muirglacier/aw/wire"
	"github.com/muirglacier/id"
)

const name = "Gossip"

type GossipTest struct{}

var _ test.Test = &GossipTest{}

func gossipTest1(testOpts test.Options) {

}

func gossipTest2(testOpts test.Options) {

}

func gossipTest3(testOpts test.Options) {

}

func (gt *GossipTest) Correctness(testOpts test.Options) {

}

func (gt *GossipTest) Perf(numPeers int, topology test.Topology, outputFilePath string, testOpts test.Options) {
	if outputFilePath == "" {
		_, b, _, _ := runtime.Caller(0)
		basepath := filepath.Dir(b)
		outputFilePath = filepath.Join(basepath, "../../output/output.txt")

	}
	fo, err := os.Create(outputFilePath)
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()

	opts, peers, tables, contentResolvers, _, transports := setup(numPeers, testOpts)

	for i := range opts {
		peers[i].Resolve(context.Background(), contentResolvers[i])
	}

	switch topology {
	case test.Star:
		createBidiStarTopology(numPeers, opts, peers, tables)
	case test.Ring:
		createBidiRingTopology(numPeers, opts, peers, tables)
	case test.Line:
		createBidiLineTopology(numPeers, opts, peers, tables)
	case test.Full:
		createBidiFullyConnectedTopology(numPeers, opts, peers, tables)
	case test.Random:
		createBidiRandomTopology(numPeers, opts, peers, tables)
	default:
		panic("Topology not supported")
	}

	for i := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), duration(testOpts.TransportTimeout))
		defer cancel()

		index := i
		go func() {
			transports[index].Receive(context.Background(), func() func(from id.Signatory, msg wire.Packet) error {
				var x int64 = 0
				go func() {
					//var seconds int64 = 0
					ticker := time.NewTicker(time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							//seconds += 1
							//fmt.Printf("Average throughput for peer %v: %v/second\n", index, x/seconds)
							_, err = fo.WriteString(fmt.Sprintf("%d %d\n", index, x))
							atomic.StoreInt64(&x, 0)
							if err != nil {
								fmt.Printf("error writing to file: %v", err)
							}
						}
					}

				}()
				return func(from id.Signatory, msg wire.Packet) error {
					atomic.AddInt64(&x, 1)
					if err := peers[index].Syncer().DidReceiveMessage(from, msg.Msg); err != nil {
						return err
					}
					if err := peers[index].Gossiper().DidReceiveMessage(from, msg.Msg); err != nil {
						return err
					}
					return nil
				}
			}())
			transports[index].Run(ctx)
		}()
	}

	timeout := duration(testOpts.GossiperTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	for iter := 0; iter < 10000; iter++ {
		select {
		case <-ctx.Done():
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
		default:
		}
		for i := range peers {
			msgHello := fmt.Sprintf(string(dhtutil.RandomContent()), peers[i].ID().String())
			contentID := id.NewHash([]byte(msgHello))
			contentResolvers[i].InsertContent(contentID[:], []byte(msgHello))
			peers[i].Gossip(ctx, contentID[:], &peer.DefaultSubnet)
		}
		println("Round", iter)
	}
	cancel()

	time.Sleep(5 * time.Second)
}
