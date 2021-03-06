package store

import (
	"context"
	"io"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	pb "github.com/aukbit/event-source-proto/es"
	"github.com/aukbit/pluto"
	"github.com/aukbit/pluto/client"
)

const (
	// EventSourceQueryClientName constant to be used as name of the event source query client connection
	EventSourceQueryClientName string = "event_source_query"

	// EventSourceCommandClientName constant to be used as name of the event source command client connection
	EventSourceCommandClientName string = "event_source_command"

	// AggregatorIDQueryKey constant to be used as the key in Query Params
	AggregatorIDQueryKey string = "AID"

	// HighestVersionQueryKey constant to be used as the key in Query Params
	HighestVersionQueryKey string = "HV"

	// LowestVersionQueryKey constant to be used as the key in Query Params
	LowestVersionQueryKey string = "LV"
)

var (
	errEventSourceClientNotAvailable = errors.New("event source client not available")
)

// Store holds aggregator state and version
type Store struct {
	State          interface{}
	Version        int64
	HighestVersion int64
	LowestVersion  int64
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
func (s *Store) LoadEvents(ctx context.Context, id string, fn ApplyFn) error {
	//
	c, ok := pluto.FromContext(ctx).Client(EventSourceQueryClientName)
	if !ok {
		return errors.Wrap(errEventSourceClientNotAvailable, EventSourceQueryClientName)
	}
	conn, err := c.Dial(client.Timeout(2 * time.Second))
	if err != nil {
		return err
	}
	defer conn.Close()
	// Define query parameters
	params := make(map[string]string)
	params[AggregatorIDQueryKey] = id
	if s.HighestVersion != 0 {
		params[HighestVersionQueryKey] = strconv.FormatInt(s.HighestVersion, 10)
	}
	if s.LowestVersion != 0 {
		params[LowestVersionQueryKey] = strconv.FormatInt(s.LowestVersion, 10)
	}
	// List
	stream, err := c.Stub(conn).(pb.EventSourceProjectionClient).List(ctx, &pb.Query{Params: params})
	if err != nil {
		return err
	}
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := s.apply(event, fn); err != nil {
			return err
		}
	}
	return nil
}

// Dispatch triggeres an event to be created
func (s *Store) Dispatch(ctx context.Context, e *pb.Event) (*pb.Ack, error) {
	// Get gRPC client from service
	c, ok := pluto.FromContext(ctx).Client(EventSourceCommandClientName)
	if !ok {
		return nil, errors.Wrap(errEventSourceClientNotAvailable, EventSourceCommandClientName)
	}
	// Establish grpc connection
	conn, err := c.Dial(client.Timeout(2 * time.Second))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return c.Stub(conn).(pb.EventSourceCommandClient).Create(ctx, e)
}

// Snapit triggeres an snapshot to be created
func (s *Store) Snapit(ctx context.Context, e *pb.Event) (*pb.Ack, error) {
	// Get gRPC client from service
	c, ok := pluto.FromContext(ctx).Client(EventSourceCommandClientName)
	if !ok {
		return nil, errors.Wrap(errEventSourceClientNotAvailable, EventSourceCommandClientName)
	}
	// Establish grpc connection
	conn, err := c.Dial(client.Timeout(2 * time.Second))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return c.Stub(conn).(pb.EventSourceCommandClient).Snap(ctx, e)
}

// Marshal takes a protocol buffer message
// and encodes it into the wire format, returning the data.
func (s *Store) Marshal() ([]byte, error) {
	// Encodes snapshot state to proto message
	return proto.Marshal(s.State.(proto.Message))
}

// apply the given event to the aggregate
func (s *Store) apply(e *pb.Event, fn ApplyFn) error {
	// given the current state apply the busines rules defined for the
	// respective event Topic
	n, err := fn(e, s.State)
	if err != nil {
		return err
	}
	// set changes to the state
	s.State = n
	// set state version the same as the aggregator
	s.Version = e.Aggregate.GetVersion()
	return nil
}
