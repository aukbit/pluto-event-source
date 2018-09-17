package store

import (
	"context"
	"errors"
	"io"
	"time"

	pb "github.com/aukbit/event-source-proto"
	"github.com/aukbit/pluto"
	"github.com/aukbit/pluto/client"
)

var (
	errEventSourceClientNotAvailable = errors.New("event source client not available")
)

// Store holds aggregator state and version
type Store struct {
	State   interface{}
	Version int32
}

// ApplyFn defines type for apply functions
type ApplyFn func(e *pb.Event, state interface{}) (interface{}, error)

// NewStore returns new store
func NewStore(aggregator interface{}) *Store {
	return &Store{
		State:   aggregator,
		Version: 0,
	}
}

// LoadEvents stream events by aggregator id and apply the required changes
// TODO: use snapshots
func (s *Store) LoadEvents(ctx context.Context, name, id string, fn ApplyFn) error {
	//
	c, ok := pluto.FromContext(ctx).Client(name)
	if !ok {
		return errEventSourceClientNotAvailable
	}
	conn, err := c.Dial(client.Timeout(2 * time.Second))
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := c.Stub(conn).(pb.EventSourceProjectionClient).List(ctx, &pb.Query{Params: map[string]string{"aID": id}})
	if err != nil {
		return err
	}
	for {
		fact, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := s.apply(fact, fn); err != nil {
			return err
		}
	}
	return nil
}

// apply the given event to the aggregate
func (s *Store) apply(e *pb.Event, fn ApplyFn) error {
	s.Version++
	i, err := fn(e, s.State)
	if err != nil {
		return err
	}
	s.State = i
	return nil
}
