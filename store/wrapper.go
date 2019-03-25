package store

import (
	"errors"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/aukbit/event-source-proto/es"
	plutoClt "github.com/aukbit/pluto/client"
	"github.com/golang/protobuf/proto"
)

var (
	ErrEventWithoutAggregate = errors.New("event can not contain nil as aggregate")
	ErrInvalidAggregateId    = errors.New("event can not have an empty string as aggregateID")
	ErrInvalidVersion        = errors.New("event can not have 0 as version")
)

// HookFn type
type HookFn func(ctx context.Context, e *pb.Event, prevState, nextState interface{}) error

// ActionWrapper loads an agregator current state and previous.
// Hook functions should be used to trigger any subsequent business rules
// Or just simple cache the state of the aggregator
func ActionWrapper(aggregator interface{}, aFn ApplyFn, hFn ...HookFn) Action {
	return func(ctx context.Context, e *pb.Event) error {

		// Verify event aggregate
		if e.GetAggregate() == nil {
			return ErrEventWithoutAggregate
		}

		if e.Aggregate.GetId() == "" {
			return ErrInvalidAggregateId
		}

		if e.Aggregate.GetVersion() == 0 {
			return ErrInvalidVersion
		}

		//
		id := e.Aggregate.GetId()

		// Initialize new store
		s := NewStore(aggregator)
		// NOTE: we will apply the changes of the event here. We may want to compare the
		// previous state with the new event and apply diffrent rules.
		// To be able to do this we aggregate all events up to the previous version of the current event
		s.LowestVersion = 1
		s.HighestVersion = e.Aggregate.GetVersion() - 1

		// Load store
		if err := s.LoadEvents(ctx, id, aFn); err != nil {
			return err
		}

		// Create a copy of the state
		prevState := proto.Clone(s.State.(proto.Message))

		// Apply event received in previous state to get the next state
		nextState, err := aFn(e, s.State)
		if err != nil {
			return err
		}

		// Run hook functions
		for _, f := range hFn {
			err := f(ctx, e, prevState, nextState)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// -----------------------------------------------------------------------------

// SnapshotActionWrapper loads an agregator current state. Takes a snapshot every
// number events (nEvents)
func SnapshotActionWrapper(aggregator proto.Message, aFn ApplyFn, nEvents int64) Action {
	return func(ctx context.Context, e *pb.Event) error {

		// Verify event aggregate
		if e.GetAggregate() == nil {
			return ErrEventWithoutAggregate
		}

		if e.Aggregate.GetId() == "" {
			return ErrInvalidAggregateId
		}

		if e.Aggregate.GetVersion() == 0 {
			return ErrInvalidVersion
		}

		if !IsSnapshotTime(e, nEvents) {
			return nil
		}

		if err := TakeSnapshot(ctx, e, aggregator, aFn); err != nil {
			return err
		}

		return nil
	}
}

// NewEventSourceQueryClient wrapper for a event source query grpc pluto client
func NewEventSourceQueryClient(target string) *plutoClt.Client {
	return plutoClt.New(
		plutoClt.Name(EventSourceQueryClientName),
		plutoClt.GRPCRegister(func(cc *grpc.ClientConn) interface{} {
			return pb.NewEventSourceProjectionClient(cc)
		}),
		plutoClt.Target(target),
	)
}

// NewEventSourceCommandClient wrapper for a event source command grpc pluto client
func NewEventSourceCommandClient(target string) *plutoClt.Client {
	return plutoClt.New(
		plutoClt.Name(EventSourceCommandClientName),
		plutoClt.GRPCRegister(func(cc *grpc.ClientConn) interface{} {
			return pb.NewEventSourceCommandClient(cc)
		}),
		plutoClt.Target(target),
	)
}
