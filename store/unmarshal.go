package store

import (
	"fmt"

	"github.com/pkg/errors"

	pb "github.com/aukbit/event-source-proto"
	"github.com/golang/protobuf/proto"
)

var (
	errFormatNotSupported = errors.New("format not supported")
)

// UnmarshalEventData gets the unserialized data from the event
func UnmarshalEventData(e *pb.Event, out interface{}) error {
	switch e.Aggregate.Format {
	case pb.Aggregate_PROTOBUF:
		err := proto.Unmarshal(e.Aggregate.Data, out.(proto.Message))
		if err != nil {
			return err
		}
	default:
		return errors.Wrap(errFormatNotSupported, fmt.Sprintf("format: %v", e.Aggregate.GetFormat()))
	}
	return nil
}
