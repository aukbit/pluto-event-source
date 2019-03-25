package store

import (
	"github.com/rs/zerolog"
	context "golang.org/x/net/context"

	"google.golang.org/grpc/metadata"
)

// contextKey is a value for use with context.WithValue. It's used as
// a pointer so it fits in an interface{} without allocation.
type contextKey struct {
	name string
}

// FromContextAny returns from context the interface value to which the key is
// associated.
func FromContextAny(ctx context.Context, key string) interface{} {
	// Try to get key from http request Context
	if v := ctx.Value(contextKey{key}); v != nil {
		return v
	}
	// Try to get key from grpc IncomingContext
	if v, ok := fromIncomingContextAny(ctx, key); ok {
		return v
	}
	return nil
}

// WithContextAny returns a copy of parent ctx in which the value associated
// with key is val.
func WithContextAny(ctx context.Context, key string, val interface{}) context.Context {
	return context.WithValue(ctx, contextKey{key}, val)
}

func fromIncomingContextAny(ctx context.Context, key string) (interface{}, bool) {
	// get actorID from incoming context or generate new one
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	if _, ok := md[key]; !ok {
		return "", false
	}
	return md[key][0], true
}

func toOutgoingContext(ctx context.Context, key string, value string) context.Context {
	// add eid to outgoing context
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	md = md.Copy()
	md = metadata.Join(md, metadata.Pairs(key, value))
	ctx = metadata.NewOutgoingContext(ctx, md)
	return ctx
}

func updateContext(ctx context.Context, eid string) context.Context {
	logger := zerolog.Ctx(ctx)
	// add eid to normal context
	ctx = WithContextAny(ctx, "eid", eid)
	// create new outgoing context with eid in Metadata to be used within grpc calls
	ctx = toOutgoingContext(ctx, "eid", eid)
	// add eid to current Logger
	sublogger := logger.With().Str("eid", eid).Logger()
	// update context with new logger
	return sublogger.WithContext(ctx)
}
