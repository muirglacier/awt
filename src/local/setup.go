package local

import (
	"awt/test"
	"context"
	"fmt"
	"github.com/muirglacier/aw/channel"
	"github.com/muirglacier/aw/dht"
	"github.com/muirglacier/aw/handshake"
	"github.com/muirglacier/aw/peer"
	"github.com/muirglacier/aw/transport"
	"github.com/muirglacier/aw/wire"
	"github.com/muirglacier/id"
	"go.uber.org/zap"
	"time"
)

func duration(num int) time.Duration {
	return time.Duration(num) * time.Second
}

func createBidiRingTopology(n int, opts []peer.Options, peers []*peer.Peer, tables []dht.Table) {
	for i := range peers {
		peers[i].Link(peers[(i+1)%n].ID())
		peers[(i+1)%n].Link(peers[i].ID())

		tables[i].AddPeer(opts[(i+1)%n].PrivKey.Signatory(),
			wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(7777+((i+1)%n))), uint64(time.Now().UnixNano())))
		tables[(i+1)%n].AddPeer(opts[i].PrivKey.Signatory(),
			wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(7777+i)), uint64(time.Now().UnixNano())))
	}
}

func createBidiLineTopology(n int, opts []peer.Options, peers []*peer.Peer, tables []dht.Table) {
	for i := range peers {
		if i < n-1 {
			peers[i].Link(peers[i+1].ID())
			peers[i+1].Link(peers[i].ID())

			tables[i].AddPeer(opts[i+1].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(7777+i+1)), uint64(time.Now().UnixNano())))
			tables[i+1].AddPeer(opts[i].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(7777+i)), uint64(time.Now().UnixNano())))
		}
	}
}

func createBidiStarTopology(n int, opts []peer.Options, peers []*peer.Peer, tables []dht.Table) {
	for i := range peers {
		if i != 0 {
			peers[0].Link(peers[i].ID())
			peers[i].Link(peers[0].ID())

			tables[i].AddPeer(opts[0].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(7777)), uint64(time.Now().UnixNano())))
			tables[0].AddPeer(opts[i].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(7777+i)), uint64(time.Now().UnixNano())))

		}
	}
}

func createBidiRandomTopology(n int, opts []peer.Options, peers []*peer.Peer, tables []dht.Table) {
	panic("unimplemented")
}

func createBidiFullyConnectedTopology(n int, opts []peer.Options, peers []*peer.Peer, tables []dht.Table) {
	for i := range peers {
		for j := range peers {
			if i != j {
				peers[i].Link(peers[j].ID())

				tables[i].AddPeer(opts[j].PrivKey.Signatory(),
					wire.NewUnsignedAddress(wire.TCP,
						fmt.Sprintf("%v:%v", "localhost", uint16(7777+j)), uint64(time.Now().UnixNano())))
			}
		}
	}
}

func setup(numPeers int, testOpts test.Options) ([]peer.Options, []*peer.Peer, []dht.Table, []dht.ContentResolver, []*channel.Client, []*transport.Transport) {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.PanicLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	// Init options for all peers.
	opts := make([]peer.Options, numPeers)
	for i := range opts {
		i := i
		opts[i] = peer.DefaultOptions().WithLogger(logger).
			WithGossiperOptions(peer.DefaultGossiperOptions().WithLogger(logger)).
			WithSyncerOptions(peer.DefaultSyncerOptions().WithLogger(logger))

		opts[i].GossiperOptions.Timeout = duration(testOpts.GossiperOptsTimeout)

		opts[i].SyncerOptions.WiggleTimeout = duration(testOpts.SyncerWiggleTimeout)
	}

	peers := make([]*peer.Peer, numPeers)
	tables := make([]dht.Table, numPeers)
	contentResolvers := make([]dht.ContentResolver, numPeers)
	clients := make([]*channel.Client, numPeers)
	transports := make([]*transport.Transport, numPeers)
	for i := range peers {
		self := opts[i].PrivKey.Signatory()
		h := handshake.Filter(func(id.Signatory) error { return nil }, handshake.ECIES(opts[i].PrivKey))
		clients[i] = channel.NewClient(
			channel.DefaultOptions().
				WithLogger(logger),
			self)
		tables[i] = dht.NewInMemTable(self)
		contentResolvers[i] = dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
		transports[i] = transport.New(
			transport.DefaultOptions().
				WithLogger(logger).
				WithClientTimeout(duration(testOpts.ClientTimeout)).
				WithOncePoolOptions(handshake.DefaultOncePoolOptions().WithMinimumExpiryAge(duration(testOpts.OncePoolTimeout))).
				WithPort(uint16(7777+i)),
			self,
			clients[i],
			h,
			tables[i])
		peers[i] = peer.New(
			opts[i],
			transports[i])
		peers[i].Resolve(context.Background(), contentResolvers[i])
	}
	return opts, peers, tables, contentResolvers, clients, transports
}
