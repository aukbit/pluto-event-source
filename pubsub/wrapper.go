package pubsub

import (
	"errors"
	"fmt"

	context "golang.org/x/net/context"

	pb "github.com/aukbit/event-source-proto"
	"github.com/aukbit/pluto"
	"github.com/aukbit/pluto-event-source/store"
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
func ActionWrapper(aggregator interface{}, aFn store.ApplyFn, hFn ...HookFn) Action {
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
		s := store.NewStore(aggregator)
		// NOTE: we will apply the changes of the event here. We may want to compare the
		// previous state with the new event and apply diffrent rules.
		// To be able to do this we aggregate all events up to the previous version of the current event
		s.LowestVersion = 1
		s.HighestVersion = e.Aggregate.GetVersion() - 1

		// Load store
		if err := s.LoadEvents(ctx, id, aFn); err != nil {
			return err
		}
		prevState := s.State

		// Apply event received in previous state to get the next state
		nextState, err := aFn(e, prevState)
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

const (
	// SnapshotCreated topic
	SnapshotCreated = "snapshot_created"
)

// SnapshotActionWrapper loads an agregator current state. Takes a snapshot every
// number events (nEvents)
func SnapshotActionWrapper(aggregator proto.Message, aFn store.ApplyFn, nEvents int64) Action {
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

		if !isSnapshotTime(e.Aggregate.GetVersion(), nEvents) {
			return nil
		}

		if err := takeSnapshot(ctx, e, aggregator, aFn); err != nil {
			return err
		}

		return nil
	}
}

func isSnapshotTime(version, factor int64) bool {
	return (version % factor) == 0
}

func takeSnapshot(ctx context.Context, e *pb.Event, aggregator proto.Message, aFn store.ApplyFn) error {

	// Initialize new store
	s := store.NewStore(aggregator)
	// Define version interval
	s.LowestVersion = 1
	s.HighestVersion = e.Aggregate.GetVersion()
	// Load events from event store
	if err := s.LoadEvents(ctx, e.Aggregate.GetId(), aFn); err != nil {
		return err
	}

	// Encodes aggregator state to proto message
	data, err := s.Marshal()
	if err != nil {
		return err
	}

	// Create an event with the snapshot data
	snap := &pb.Event{
		Aggregate: &pb.Aggregate{
			Id:      e.Aggregate.GetId(),
			Schema:  fmt.Sprintf("%T", aggregator),
			Format:  pb.Aggregate_PROTOBUF,
			Data:    data,
			Version: e.Aggregate.GetVersion(),
		},

		OriginName: pluto.FromContext(ctx).Name(),
		OriginIp:   "127.0.0.1", // TODO get OriginIp from service
	}

	// Snap event
	if _, err := s.Snapit(ctx, snap); err != nil {
		return err
	}

	return nil
}
