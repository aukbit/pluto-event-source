package snapshot

import (
	"fmt"

	context "golang.org/x/net/context"

	pb "github.com/aukbit/event-source-proto"
	"github.com/aukbit/pluto"
	store "github.com/aukbit/pluto-event-source/store"
	"github.com/golang/protobuf/proto"
)

// -----------------------------------------------------------------------------

const (
	// SnapshotCreated topic
	SnapshotCreated = "snapshot_created"
)

// IsSnapshotTime validate if event version is valid to be used as snapshot based on
// input factor
func IsSnapshotTime(e *pb.Event, factor int64) bool {
	if e.Aggregate.GetVersion() == 0 {
		return false
	}
	if factor == 0 {
		return false
	}
	return (e.Aggregate.GetVersion() % factor) == 0
}

// TakeSnapshot loads an agregator up to current state and triggers a snapshot
func TakeSnapshot(ctx context.Context, e *pb.Event, aggregator proto.Message, aFn store.ApplyFn) error {

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
