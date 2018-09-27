package pubsub

import (
	"errors"

	context "golang.org/x/net/context"

	pb "github.com/aukbit/event-source-proto"
	"github.com/aukbit/pluto-event-source/store"
)

var (
	ErrEventWithoutAggregate = errors.New("event can not contain nil as aggregate")
	ErrInvalidAggregateId    = errors.New("event can not have an empty string as aggregateID")
	ErrInvalidVersion        = errors.New("event can not have 0 as version")
)

// GetAggregateIDFn type
type GetAggregateIDFn func(e *pb.Event) (string, error)

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
