package store

import (
	"fmt"

	context "golang.org/x/net/context"

	pb "github.com/aukbit/event-source-proto/es"
	"github.com/aukbit/pluto"
	"github.com/aukbit/pluto/server"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/metadata"
)

// Validate helper functions to validate the aggregate
type Validate func(*Store) error

// Aggregate proceess all aggregate steps
func Aggregate(ctx context.Context, aggregator interface{}, id string, in proto.Message, topic string, metadata map[string]string, apply ApplyFn, validations ...Validate) (*Store, error) {
	logger := zerolog.Ctx(ctx)

	// Initialize aggregator store
	s := NewStore(aggregator)

	// Load events into store
	if err := s.LoadEvents(ctx, id, apply); err != nil {
		return nil, err
	}

	// Run validations
	for _, v := range validations {
		if err := v(s); err != nil {
			return nil, err
		}
	}

	// Encodes input message
	data, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Create an event
	e := &pb.Event{
		Topic: topic,

		Aggregate: &pb.Aggregate{
			Id:       id,
			Schema:   fmt.Sprintf("%T", in),
			Format:   pb.Aggregate_PROTOBUF,
			Data:     data,
			Version:  s.Version,
			Metadata: metadata,
		},

		OriginName: pluto.FromContext(ctx).Name(),
		OriginIp:   "127.0.0.1",
	}

	if eid := eidFromIncomingContext(ctx); eid != "" {
		e.Metadata = map[string]string{"eid": eid}
	}

	// Dispatch event
	if _, err := s.Dispatch(ctx, e); err != nil {
		return nil, err
	}

	// Apply last event to the aggregator
	if err := s.apply(e, apply); err != nil {
		return nil, err
	}
	logger.Info().Msg(fmt.Sprintf("state: %v", s.State))
	return s, nil
}

// ---

// eidFromIncomingContext returns eid from incoming context
func eidFromIncomingContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		s := server.FromContext(ctx)
		l := s.Logger()
		l.Warn().Msg("metadata not available in incoming context")
		return ""
	}
	_, ok = md["eid"]
	if !ok {
		s := server.FromContext(ctx)
		l := s.Logger()
		l.Warn().Msg("eid not available in metadata")
		return ""
	}
	return md["eid"][0]
}
