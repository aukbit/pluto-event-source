package store

import (
	"fmt"

	context "golang.org/x/net/context"

	pb "github.com/aukbit/event-source-proto/es"
	"github.com/aukbit/pluto"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrConcurrencyException = status.Error(codes.Aborted, "concurrency exception")

// Validate helper functions to validate the aggregate
type Validate func(*Store) error

// Aggregate proceess all aggregate steps
func Aggregate(ctx context.Context, aggregator interface{}, id string, in proto.Message, topic string, metadata map[string]string, apply ApplyFn, validations ...Validate) (*Store, error) {
	l := zerolog.Ctx(ctx)

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

	if eid, ok := FromContextAny(ctx, "eid").(string); ok {
		e.Metadata = map[string]string{"eid": eid}
	}

	// Dispatch event
	if _, err := s.Dispatch(ctx, e); err != nil {
		st := status.Convert(err)
		switch st.Code() {
		case codes.Aborted:
			l.Warn().Msgf("event %s with %s version %d  will try again got error %v", e.GetTopic(), e.Aggregate.GetId(), e.Aggregate.GetVersion(), st.Message())
			return Aggregate(ctx, aggregator, id, in, topic, metadata, apply, validations...)
		default:
			return nil, err
		}
	}

	// Apply last event to the aggregator
	if err := s.apply(e, apply); err != nil {
		return nil, err
	}
	l.Info().Msg(fmt.Sprintf("state: %v", s.State))
	return s, nil
}
