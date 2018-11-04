package pluto

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aukbit/fibonacci"
	"github.com/aukbit/pluto/client"
	"github.com/aukbit/pluto/common"
	"github.com/aukbit/pluto/server"
	"github.com/aukbit/pluto/server/router"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultName       = "pluto"
	defaultHealthAddr = ":9090"
)

var (
	ErrDatastoreNotInitialized = errors.New("datastore not initialized")
)

// Service representacion of a pluto service
type Service struct {
	close  chan bool
	cfg    Config
	wg     *sync.WaitGroup
	health *health.Server
	logger zerolog.Logger
}

func init() {
	zerolog.TimestampFieldName = "timestamp"
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.LevelFieldName = "severity"
	zerolog.MessageFieldName = "message"
	switch os.Getenv("LOGGER_LEVEL") {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

}

// New returns a new pluto service with Options passed in
func New(opts ...Option) *Service {
	return newService(opts...)
}

func newService(opts ...Option) *Service {
	s := &Service{
		cfg:    newConfig(),
		close:  make(chan bool),
		wg:     &sync.WaitGroup{},
		health: health.NewServer(),
	}
	s.logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	if len(opts) > 0 {
		s = s.WithOptions(opts...)
	}
	return s
}

// WithOptions clones the current Service, applies the supplied Options, and
// returns the resulting Service. It's safe to use concurrently.
func (s *Service) WithOptions(opts ...Option) *Service {
	for _, opt := range opts {
		opt.apply(s)
	}
	return s
}

// Run starts service
func (s *Service) Run() error {
	s.logger = s.logger.With().Str("id", s.cfg.ID).Str("name", s.cfg.Name).Logger()
	// set health server
	s.setHealthServer()
	// start service
	if err := s.start(); err != nil {
		return err
	}
	// hook run after start
	if err := s.hookAfterStart(); err != nil {
		return err
	}
	// wait for all go routines to finish
	s.wg.Wait()
	s.logger.Warn().Msg(fmt.Sprintf("%s has just exited", s.Name()))
	return nil
}

// Stop stops service
func (s *Service) Stop() {
	s.logger.Info().Msg(fmt.Sprintf("shutting down %s", s.Name()))
	s.close <- true
}

// Push allows to start additional options while service is running
func (s *Service) Push(opts ...Option) {
	for _, opt := range opts {
		opt.apply(s)
	}
}

// Server returns a server instance by name if initialized in service
func (s *Service) Server(name string) (srv *server.Server, ok bool) {
	name = common.SafeName(name, server.DefaultName)
	if srv, ok = s.cfg.Servers[name]; !ok {
		return
	}
	return srv, true
}

// Client returns a client instance by name if initialized in service
func (s *Service) Client(name string) (clt *client.Client, ok bool) {
	name = common.SafeName(name, client.DefaultName)
	if clt, ok = s.cfg.Clients[name]; !ok {
		return
	}
	return clt, true
}

// Health ...
func (s *Service) Health() *healthpb.HealthCheckResponse {
	hcr, err := s.health.Check(
		context.Background(), &healthpb.HealthCheckRequest{Service: s.cfg.ID})
	if err != nil {
		s.logger.Error().Msg(err.Error())
	}
	return hcr
}

// ID returns service id
func (s *Service) ID() string {
	return s.cfg.ID
}

// Name returns service name
func (s *Service) Name() string {
	return s.cfg.Name
}

// Logger returns service logger
func (s *Service) Logger() zerolog.Logger {
	return s.logger
}

// WaitUntilFinish returns service name
func (s *Service) WaitUntilFinish() {
	<-s.close
}

func (s *Service) setHealthServer() {
	s.health.SetServingStatus(s.cfg.ID, 1)
	// Define Router
	mux := router.New()
	mux.GET("/_health/:module/:name", healthHandler)
	// Define server
	srv := server.New(
		server.Name(s.cfg.Name+"_health"),
		server.Addr(s.cfg.HealthAddr),
		server.Mux(mux),
		// server.Logger(s.logger),
	)
	s.cfg.Servers[srv.Name()] = srv
}

func (s *Service) start() error {
	s.logger.Info().Str("ip4", common.IPaddress()).
		Int("servers", len(s.cfg.Servers)).
		Int("clients", len(s.cfg.Clients)).
		Msg(fmt.Sprintf("starting %s, servers: %d clients: %d", s.Name(), len(s.cfg.Servers), len(s.cfg.Clients)))
	// run servers
	s.startServers()
	// dial clients
	s.startClients()
	// add go routine to WaitGroup
	s.wg.Add(1)
	go s.waitUntilStopOrSig()
	return nil
}

func (s *Service) hookAfterStart() error {
	hooks, ok := s.cfg.Hooks["after_start"]
	if !ok {
		return nil
	}
	ctx := context.Background()
	// make service available in hooks context
	ctx = s.WithContext(ctx)
	// make logger available in hooks context
	sublogger := s.logger.With().Str("hook", "after_start").Logger()
	ctx = sublogger.WithContext(ctx)
	for _, h := range hooks {
		if err := h(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) startServers() {
	for _, srv := range s.cfg.Servers {
		// add go routine to WaitGroup
		s.wg.Add(1)
		go func(s *Service, srv *server.Server) {
			defer s.wg.Done()
			f := fibonacci.F()
			for {
				err := srv.Run(
					server.Middlewares(
						serviceContextMiddleware(s),
					),
					server.UnaryServerInterceptors(
						serviceContextUnaryServerInterceptor(s),
					),
					server.StreamServerInterceptors(
						serviceContextStreamServerInterceptore(s),
					),
					server.Logger(s.logger),
				)
				if err == nil {
					return
				}
				l := srv.Logger()
				l.Error().Msg(fmt.Sprintf("%v failed to start, error: %v", srv.Name(), err.Error()))
				time.Sleep(time.Duration(f()) * time.Second)
			}
		}(s, srv)
	}
}

// startClients listen to the clientsCh
func (s *Service) startClients() {
	go func() {
		for {
			select {
			case clt, ok := <-s.cfg.clientsCh:
				if !ok {
					break
				}
				s.startClient(clt)
			default:
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}
	}()
}

func (s *Service) startClient(clt *client.Client) {
	go func(clt *client.Client) {
		clt.Init(client.Logger(s.logger))
	}(clt)
}

// waitUntilStopOrSig waits for close channel or syscall Signal
func (s *Service) waitUntilStopOrSig() {
	defer s.wg.Done()
	//  Stop also in case of any host signal
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGTERM, syscall.SIGINT)

outer:
	for {
		select {
		case <-s.close:
			// Waits for call to stop
			s.health.SetServingStatus(s.cfg.ID, 2)
			s.unregister()
			s.closeClients()
			s.stopServers()
			break outer
		case sig := <-sigch:
			// Waits for signal to stop
			s.logger.Info().Msg(fmt.Sprintf("shutting down, got signal: %v", sig))
			s.health.SetServingStatus(s.cfg.ID, 2)
			s.unregister()
			s.closeClients()
			s.stopServers()
			break outer
		default:
			// keep on looping, non-blocking channel operations
			time.Sleep(50 * time.Millisecond)
			continue
		}
	}
}

func (s *Service) closeClients() {
	close(s.cfg.clientsCh)
	// for _, clt := range s.cfg.Clients {
	// 	// add go routine to WaitGroup
	// 	s.wg.Add(1)
	// 	go func(clt *client.Client) {
	// 		defer s.wg.Done()
	// 		clt.Close()
	// 	}(clt)
	// }
}

func (s *Service) stopServers() {
	for _, srv := range s.cfg.Servers {
		// add go routine to WaitGroup
		s.wg.Add(1)
		go func(s *Service, srv *server.Server) {
			defer s.wg.Done()
			srv.Stop()
		}(s, srv)
	}
}
