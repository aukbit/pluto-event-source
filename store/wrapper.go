package store

import (
	pb "github.com/aukbit/event-source-proto"
	plutoClt "github.com/aukbit/pluto/client"
	"google.golang.org/grpc"
)

// NewEventSourceQueryClient wrapper for a event source query grpc pluto client
func NewEventSourceQueryClient(target string) *plutoClt.Client {
	return plutoClt.New(
		plutoClt.Name(EventSourceQueryClientName),
		plutoClt.GRPCRegister(func(cc *grpc.ClientConn) interface{} {
			return pb.NewEventSourceCommandClient(cc)
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
