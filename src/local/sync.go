package local

import (
	"awt/test"
	"bytes"
	"context"
	"fmt"
	"github.com/muirglacier/aw/dht/dhtutil"
	"github.com/muirglacier/id"
	"sync/atomic"
	"time"
)

type SyncTest struct {}
var _ test.Test = &SyncTest{}

func syncTest1(testOpts test.Options) {
	num := 5
	opts, peers, tables, contentResolvers, _, _ := setup(num, testOpts)

	for i := range peers {
		peers[i].Resolve(context.Background(), contentResolvers[i])
	}

	createBidiFullyConnectedTopology(num, opts, peers, tables)

	ctx, cancel := context.WithTimeout(context.Background(), 20 * time.Second)
	defer cancel()
	for i := range peers {
		go peers[i].Run(ctx)
	}

	msgX := "Hello Nodes! I have a secret for you"
	msgY := "My name's Node 0"
	msgZ := "My secret is - I'm Batman"
	contentIDX := id.NewHash([]byte(msgX))
	contentIDY := id.NewHash([]byte(msgY))
	contentIDZ := id.NewHash([]byte(msgZ))
	contentResolvers[0].InsertContent(contentIDX[:], []byte(msgX))
	contentResolvers[0].InsertContent(contentIDY[:], []byte(msgY))
	contentResolvers[0].InsertContent(contentIDZ[:], []byte(msgZ))

	gossipCtx, gossipCancel := context.WithTimeout(ctx, 5 * time.Second)
	defer gossipCancel()
	peers[0].Gossip(gossipCtx, contentIDX[:], nil)
	time.Sleep(time.Second)

	for i := range peers {
		contentX , ok := contentResolvers[i].QueryContent(contentIDX[:])
		if !ok {
			fmt.Println(string(colorRed), "[ ] - Follow up information sync test failed")
			panic("initial gossiped content not received\n\n")
		}
		if string(contentX) != msgX {
			panic("gossipped content not synced correctly")
		}
	}
	syncCtx, syncCancel := context.WithTimeout(ctx, 20 * time.Second)
	endChan := make(chan struct{}, num-1)
	defer syncCancel()
	for i := range peers {
		if i != 0 {
			go func() {
				peerID := peers[0].ID()
				syncedMsgY, err := peers[i].Sync(syncCtx, contentIDY[:], &peerID)
				if err != nil {
					panic(fmt.Sprintf("error when syncing 2nd content: %v", err))
				}
				if string(syncedMsgY) != msgY {
					panic("synced content is incorrect")
				}
				syncedMsgZ, err := peers[i].Sync(syncCtx, contentIDZ[:], &peerID)
				if err != nil {
					panic(fmt.Sprintf("error when syncing 2nd content: %v", err))
				}
				if string(syncedMsgZ) != msgZ {
					panic("synced content is incorrect")
				}

				endChan <- struct{}{}
			}()
		}
	}

	channelCount := 0

	for channelCount != num -1 {
		select {
		case <-endChan:
			channelCount++
		case <-syncCtx.Done():
			fmt.Println(string(colorRed), "[ ] - Follow up information sync test failed")
			fmt.Println(string(colorReset))
			panic("Context cancelled before successful syncing\n\n")
		}
	}

	fmt.Println(string(colorGreen), "[x] - Follow up information sync test ran successfully\n\n")
	fmt.Println(string(colorReset))
}

func syncTest2(testOpts test.Options) {
	num := 20
	opts, peers, tables, contentResolvers, _, _ := setup(num, testOpts)

	for i := range peers {
		peers[i].Resolve(context.Background(), contentResolvers[i])
	}

	createBidiFullyConnectedTopology(num, opts, peers, tables)

	ctx, cancel := context.WithTimeout(context.Background(), 100 * time.Second)
	defer cancel()
	for i := range peers {
		go peers[i].Run(ctx)
	}

	var syncCount int64 = 0

	var totalMsgs int64 = int64(1 * (num - 1) * num)
	endChan := make(chan struct{}, totalMsgs)
	startTime := time.Now()
	for round := 0; round < 1; round++ {
		for i := range peers {
			newMsg := dhtutil.RandomContent()
			hash := id.NewHash(newMsg)
			contentResolvers[i].InsertContent(hash[:], newMsg)

			for j := range peers {
				if i != j {
					iCopy := i
					jCopy := j
					go func() {
						syncCtx, syncCancel := context.WithTimeout(ctx, duration(testOpts.SyncerTimeout))
						defer syncCancel()
						hint := peers[iCopy].ID()
						syncedMsg, err := peers[jCopy].Sync(syncCtx, hash[:], &hint)
						if err == nil && bytes.Equal(syncedMsg, newMsg) {
							atomic.AddInt64(&syncCount, 1)
						} else {
							fmt.Printf("Syncer %v failed to sync with peer %v with error: %v\n", jCopy, iCopy, err)
						}
						endChan <- struct{}{}
					}()
				}
			}
		}
	}

	var channelCount int64 = 0
	for channelCount != totalMsgs {
		select {
		case <-endChan:
			channelCount++
		case <-ctx.Done():
			fmt.Println(string(colorRed), "[ ] - Follow up information sync test failed")
			fmt.Println(string(colorReset))
			panic("Context cancelled before successful syncing\n\n")
		}
	}

	endTime := time.Now()
	totalTime := endTime.Unix() - startTime.Unix()

	if syncCount != totalMsgs {
		fmt.Println(string(colorRed), syncCount,"/",totalMsgs, " synced in ", totalTime," seconds\n\n")
		fmt.Println(string(colorReset))
	} else {
		fmt.Println(string(colorRed), "All messages (", totalMsgs, ") synced in ", totalTime," seconds\n\n")
		fmt.Println(string(colorReset))
	}
}

func (st *SyncTest) Correctness(testOpts test.Options) {
	syncTest1(testOpts)
	syncTest2(testOpts)
}


// Performance test

func (st *SyncTest) Perf(num int, topology test.Topology, outputName string, testOpts test.Options) {
	panic(fmt.Sprintf("No perf tests for %v test", name))
}
