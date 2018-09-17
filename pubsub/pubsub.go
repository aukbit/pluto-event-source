package pubsub

import (
	// "context"

	"fmt"
	"os"
	"strings"

	context "golang.org/x/net/context"

	"cloud.google.com/go/pubsub"
	pb "github.com/aukbit/event-source-proto"
	"github.com/aukbit/pluto"
	"github.com/rs/zerolog"
)

// Action signature of a action func
type Action func(context.Context, *pb.Event) error

// Topics map between a topic and respective action
type Topics map[string][]Action

// Subscribe for topics available from a redis configuration
func Subscribe(client *pubsub.Client, name string, topics Topics) pluto.HookFunc {
	return func(ctx context.Context) error {
		l := zerolog.Ctx(ctx)
		// Set project environment as topic prefix eg. development.event_created
		env, ok := os.LookupEnv("GCP_PROJECT_ENV")
		if !ok {
			return errGcpProjectEnvironmentNotDefined
		}
		l.Info().Msgf("subscribe to topics: %v", topics)
		//
		for t, actions := range topics {
			t = strings.ToLower(t)
			topic, err := GetOrCreateTopic(ctx, client, t)
			if err != nil {
				return err
			}
			// update topic labels
			_, err = UpdateTopic(ctx, topic)
			if err != nil {
				return err
			}
			// subscribe
			n := fmt.Sprintf("%s.%s.%s", env, name, t)
			sub, err := GetOrCreateSubscription(ctx, client, n, topic)
			if err != nil {
				return err
			}
			go pullMsgsFromSubscription(ctx, sub, actions)
		}
		return nil
	}
}
