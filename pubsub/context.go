package pubsub

import (
	context "golang.org/x/net/context"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/metadata"
)

func updateContext(ctx context.Context, eid string) context.Context {
	logger := zerolog.Ctx(ctx)
	// create new outgoing context with eid in Metadata to be used within grpc calls
	md := metadata.New(map[string]string{})
	md = metadata.Join(md, metadata.Pairs("eid", eid))
	ctx = metadata.NewOutgoingContext(ctx, md)
	// add eid to current Logger
	sublogger := logger.With().Str("eid", eid).Logger()
	// update context with new logger
	return sublogger.WithContext(ctx)
}
