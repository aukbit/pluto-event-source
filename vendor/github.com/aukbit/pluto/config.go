package pluto

import (
	context "golang.org/x/net/context"

	"github.com/aukbit/pluto/client"
	"github.com/aukbit/pluto/common"
	"github.com/aukbit/pluto/discovery"
	"github.com/aukbit/pluto/server"
)

// Config pluto service config
type Config struct {
	ID          string
	Name        string
	Description string
	clientsCh   chan *client.Client
	Discovery   discovery.Discovery
	HealthAddr  string // TCP address (e.g. localhost:8000) to listen on, ":http" if empty
	Servers     map[string]*server.Server
	Clients     map[string]*client.Client
	Hooks       map[string][]HookFunc
}

// HookFunc hook function type
type HookFunc func(context.Context) error

func newConfig() Config {
	return Config{
		ID:         common.RandID("plt_", 6),
		Name:       defaultName,
		Servers:    make(map[string]*server.Server),
		Clients:    make(map[string]*client.Client),
		clientsCh:  make(chan *client.Client, 100),
		Hooks:      make(map[string][]HookFunc),
		HealthAddr: defaultHealthAddr,
	}
}
