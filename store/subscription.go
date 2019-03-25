package store

import (
	"os"
	"time"

	context "golang.org/x/net/context"

	"cloud.google.com/go/pubsub"
	pb "github.com/aukbit/event-source-proto/es"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
)

// GetOrCreateSubscription gets a reference or creates a Cloud PubSub subscription for the input topic
func GetOrCreateSubscription(ctx context.Context, client *pubsub.Client, name string, topic *pubsub.Topic) (*pubsub.Subscription, error) {
	l := zerolog.Ctx(ctx)
	// Verify if topic exists
	s := client.Subscription(name)
	ok, err := s.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		// [START create_subscription]
		s, err = client.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: 20 * time.Second,
			Labels:      map[string]string{"env": os.Getenv("GCP_PROJECT_ENV")},
		})
		if err != nil {
			return nil, err
		}
		l.Info().Msgf("created subscription: %v", s)
		return s, nil
	}
	// [END create_subscription]
	l.Info().Msgf("available subscription: %v", s)
	return s, nil
}

// DeleteSubscription deletes a Cloud PubSub subscription
func DeleteSubscription(ctx context.Context, client *pubsub.Client, name string) error {
	l := zerolog.Ctx(ctx)
	// [START delete_subscription]
	sub := client.Subscription(name)
	if err := sub.Delete(ctx); err != nil {
		return err
	}
	l.Info().Msgf("subscription %v deleted.", name)
	// [END delete_subscription]
	return nil
}

func pullMsgsFromSubscription(ctx context.Context, sub *pubsub.Subscription, actions []Action) {
	l := zerolog.Ctx(ctx)
	if ok, err := sub.Exists(ctx); !ok {
		l.Error().Msg(err.Error())
		return
	}
	// [START pull_messages]
	cctx, _ := context.WithCancel(ctx)
	for {
		err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
			e := &pb.Event{}
			err := proto.Unmarshal(msg.Data, e)
			if err != nil {
				// error parsing the message
				l.Error().Msg(err.Error())
				msg.Nack()
				return
			}
			// update context with eid
			ctx = updateContext(ctx, e.Metadata["eid"])
			l = zerolog.Ctx(ctx)
			l.Info().Msgf("message %v received on subscription: %v", msg.ID, sub)
			for _, a := range actions {
				if err := a(ctx, e); err != nil {
					l.Error().Msg(err.Error())
					msg.Nack()
					return
				}
			}
			msg.Ack()
		})
		if err != nil {
			// if pubsub is down or network issues - wait and try again
			l.Error().Msg(err.Error())
			time.Sleep(5 * time.Second)
		}
	}
	// [END pull_messages]
}
