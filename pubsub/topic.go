package pubsub

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/pubsub"
)

var (
	errGcpProjectEnvironmentNotDefined = errors.New("GCP_PROJECT_ENV not defined")
)

// GetOrCreateTopic create a Cloud PubSub topic if not exists
func GetOrCreateTopic(ctx context.Context, client *pubsub.Client, topic string) (*pubsub.Topic, error) {
	// Set project environment as topic prefix eg. development.event_created
	env, ok := os.LookupEnv("GCP_PROJECT_ENV")
	if !ok {
		return nil, errGcpProjectEnvironmentNotDefined
	}
	name := fmt.Sprintf("%s.%s", env, strings.ToLower(topic))
	// Verify if topic exists
	t := client.Topic(name)
	ok, err := t.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Topic doesn't exist.
		// Create a new topic with the given name.
		return client.CreateTopic(ctx, name)
	}
	return t, nil
}

// UpdateTopic updates topic environment labels
func UpdateTopic(ctx context.Context, t *pubsub.Topic) (pubsub.TopicConfig, error) {
	cfg, err := t.Config(ctx)
	if err != nil {
		return pubsub.TopicConfig{}, err
	}
	if cfg.Labels == nil {
		cfgu := pubsub.TopicConfigToUpdate{
			Labels: map[string]string{"env": os.Getenv("GCP_PROJECT_ENV")},
		}
		return t.Update(ctx, cfgu)
	}
	if _, ok := cfg.Labels["env"]; !ok {
		// append environment label
		cfgu := pubsub.TopicConfigToUpdate{
			Labels: cfg.Labels,
		}
		cfgu.Labels["env"] = os.Getenv("GCP_PROJECT_ENV")
		return t.Update(ctx, cfgu)
	}
	return cfg, nil
}
