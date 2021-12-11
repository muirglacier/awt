package remote

import (
	"awt/test"
	"context"

	"github.com/muirglacier/aw/channel"
	"github.com/muirglacier/aw/dht"
	"github.com/muirglacier/aw/handshake"
	"github.com/muirglacier/aw/peer"
	"github.com/muirglacier/aw/transport"
	"github.com/muirglacier/id"
	"go.uber.org/zap"
	"time"
)

func duration(num int) time.Duration {
	return time.Duration(num) * time.Second
}

func setup(key *id.PrivKey, testOpts test.Options) (peer.Options, *peer.Peer, dht.Table, dht.ContentResolver, *channel.Client, *transport.Transport) {
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.PanicLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	// Init options for all peers.
	opts := peer.DefaultOptions().WithPrivKey(key).WithLogger(logger).
		WithGossiperOptions(peer.DefaultGossiperOptions().WithLogger(logger).WithTimeout(2 * time.Second)).
		WithSyncerOptions(peer.DefaultSyncerOptions().WithLogger(logger).WithWiggleTimeout(2 * time.Second))

	self := opts.PrivKey.Signatory()
	h := handshake.Filter(func(id.Signatory) error { return nil }, handshake.ECIES(opts.PrivKey))
	client := channel.NewClient(
		channel.DefaultOptions().
			WithLogger(logger).
			WithMaxMessageSize(1024),
		self)
	table := dht.NewInMemTable(self)
	contentResolver := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
	t := transport.New(
		transport.DefaultOptions().
			WithHost("").
			WithLogger(logger).
			WithOncePoolOptions(handshake.DefaultOncePoolOptions().WithMinimumExpiryAge(10*time.Second)).
			WithPort(uint16(8080)),
		self,
		client,
		h,
		table)
	p := peer.New(
		opts,
		t)
	p.Resolve(context.Background(), contentResolver)

	return opts, p, table, contentResolver, client, t
}
