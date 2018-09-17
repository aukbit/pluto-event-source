package server

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aukbit/pluto/common"
	"github.com/aukbit/pluto/discovery"
	"github.com/aukbit/pluto/server/router"
	"github.com/rs/zerolog"
	"golang.org/x/net/context"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"google.golang.org/grpc"
)

const (
	// DefaultName server prefix name
	DefaultName = "server"
)

// A Server defines parameters for running an HTTP server.
// The zero value for Server is a valid configuration.
type Server struct {
	close chan bool

	cfg        Config
	wg         *sync.WaitGroup
	httpServer *http.Server
	grpcServer *grpc.Server
	health     *health.Server
	logger     zerolog.Logger
}

// New returns a new http server with cfg passed in
func New(opts ...Option) *Server {
	return newServer(opts...)
}

// newServer will instantiate a new defaultServer with the given config
func newServer(opts ...Option) *Server {
	s := &Server{
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

// WithOptions clones the current Client, applies the supplied Options, and
// returns the resulting Client. It's safe to use concurrently.
func (s *Server) WithOptions(opts ...Option) *Server {
	for _, opt := range opts {
		opt.apply(s)
	}
	return s
}

// Run Server
func (s *Server) Run(opts ...Option) error {
	// set last configs
	for _, opt := range opts {
		opt.apply(s)
	}
	s.logger = s.logger.With().Dict("server", zerolog.Dict().
		Str("id", s.cfg.ID).
		Str("name", s.cfg.Name).
		Str("format", s.cfg.Format).
		Str("port", s.cfg.Addr),
	).Logger()
	// register at service discovery
	if err := s.register(); err != nil {
		return err
	}
	// start server
	if err := s.start(); err != nil {
		return err
	}
	// set health
	s.health.SetServingStatus(s.cfg.ID, 1)
	// wait for go routines to finish
	s.wg.Wait()
	s.logger.Warn().Msg(fmt.Sprintf("%s has just exited", s.Name()))
	return nil
}

// Stop stops server by sending a message to close the listener via channel
func (s *Server) Stop() {
	// set health as not serving
	s.health.SetServingStatus(s.cfg.ID, 2)
	// close listener
	s.close <- true
}

func (s *Server) Health() *healthpb.HealthCheckResponse {
	switch s.cfg.Format {
	case "grpc":
		s.healthGRPC()
	default:
		s.healthHTTP()
	}
	hcr, err := s.health.Check(
		context.Background(), &healthpb.HealthCheckRequest{Service: s.cfg.ID})
	if err != nil {
		s.logger.Error().Msg(err.Error())
		return &healthpb.HealthCheckResponse{Status: 2}
	}
	return hcr
}

// Name returns server name
func (s *Server) Name() string {
	return s.cfg.Name
}

// Logger returns server logger
func (s *Server) Logger() zerolog.Logger {
	return s.logger
}

func (s *Server) setHTTPServer() {
	if s.cfg.Mux == nil {
		s.cfg.Mux = router.New()
	}
	// set health check handler
	s.cfg.Mux.GET("/_health", router.Wrap(healthHandler))

	s.cfg.mu.Lock()
	// append logger
	s.cfg.Middlewares = append(s.cfg.Middlewares,
		loggerMiddleware(s), eidMiddleware(s), serverMiddleware(s),
	)
	// wrap Middlewares
	s.cfg.Mux.WrapperMiddleware(s.cfg.Middlewares...)
	s.cfg.mu.Unlock()
	// initialize http server
	s.httpServer = &http.Server{
		// handler to invoke, http.DefaultServeMux if nil
		Handler: s.cfg.Mux,

		// ReadTimeout is used by the http server to set a maximum duration before
		// timing out read of the request. The default timeout is 10 seconds.
		ReadTimeout: 10 * time.Second,

		// WriteTimeout is used by the http server to set a maximum duration before
		// timing out write of the response. The default timeout is 10 seconds.
		WriteTimeout: 10 * time.Second,

		TLSConfig: s.cfg.TLSConfig,
	}
}

func (s *Server) start() (err error) {
	s.logger.Info().Msg(fmt.Sprintf("starting %s %s, listening on %s", s.cfg.Format, s.Name(), s.cfg.Addr))
	var ln net.Listener

	switch s.cfg.Format {
	case "https":
		// append strict security header
		s.cfg.mu.Lock()
		s.cfg.Middlewares = append(s.cfg.Middlewares, strictSecurityHeaderMiddleware())
		s.cfg.mu.Unlock()
		ln, err = s.listenTLS()
		if err != nil {
			return err
		}
	default:
		ln, err = s.listen()
		if err != nil {
			return err
		}
	}

	switch s.cfg.Format {
	case "grpc":
		s.setGRPCServer()
		if err := s.serveGRPC(ln); err != nil {
			return err
		}
	default:
		s.setHTTPServer()
		if err := s.serve(ln); err != nil {
			return err
		}
	}

	// add go routine to WaitGroup
	s.wg.Add(1)
	go s.waitUntilStop(ln)
	return nil
}

// listen based on http.ListenAndServe
// listens on the TCP network address srv.Addr
// If srv.Addr is blank, ":http" is used.
// returns nil or new listener
func (s *Server) listen() (net.Listener, error) {

	addr := s.cfg.Addr
	if addr == "" {
		addr = ":http"
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	ln = net.Listener(TCPKeepAliveListener{ln.(*net.TCPListener)})

	return ln, nil
}

// listenTLS based on http.ListenAndServeTLS
// listens on the TCP network address srv.Addr
// If srv.Addr is blank, ":https" is used.
// returns nil or new listener
func (s *Server) listenTLS() (net.Listener, error) {

	addr := s.cfg.Addr
	if addr == "" {
		addr = ":https"
	}

	ln, err := tls.Listen("tcp", addr, s.cfg.TLSConfig)
	if err != nil {
		return nil, err
	}
	return ln, nil
}

// serve based on http.ListenAndServe
// calls Serve to handle requests on incoming connections.
// Accepted connections are configured to enable TCP keep-alives.
// serve always returns a non-nil error.
func (s *Server) serve(ln net.Listener) error {
	// add go routine to WaitGroup
	s.wg.Add(1)
	go func(s *Server, ln net.Listener) {
		defer s.wg.Done()
		if err := s.httpServer.Serve(ln); err != nil {
			if err.Error() == errClosing(ln).Error() {
				return
			}
			s.logger.Error().Msg(err.Error())
			return
		}
	}(s, ln)
	return nil
}

// errClosing is the currently error raised when we gracefull
// close the listener. If this is the case there is no point to log
func errClosing(ln net.Listener) error {
	return fmt.Errorf("accept tcp %v: use of closed network connection", ln.Addr().String())
}

// waitUntilStop waits for close channel
func (s *Server) waitUntilStop(ln net.Listener) {
	defer s.wg.Done()
	// Waits for call to stop
	<-s.close
	s.unregister()
	switch s.cfg.Format {
	case "grpc":
		s.grpcServer.GracefulStop()
	default:
		if err := ln.Close(); err != nil {
			s.logger.Error().Msg(err.Error())
		}
	}
}

// register Server within the service discovery system
func (s *Server) register() error {
	if _, ok := s.cfg.Discovery.(discovery.Discovery); ok {
		// define service
		dse := discovery.Service{
			Name:    s.cfg.Name,
			Address: common.IPaddress(),
			Port:    s.cfg.Port(),
			Tags:    []string{s.cfg.ID},
		}
		// define check
		dck := discovery.Check{
			Name:  fmt.Sprintf("Service '%s' check", s.cfg.Name),
			Notes: fmt.Sprintf("Ensure the server is listening on port %s", s.cfg.Addr),
			DeregisterCriticalServiceAfter: "10m",
			HTTP:      fmt.Sprintf("http://%s:9090/_health/server/%s", common.IPaddress(), s.cfg.Name),
			Interval:  "30s",
			Timeout:   "1s",
			ServiceID: s.cfg.Name,
		}
		if err := s.cfg.Discovery.Register(discovery.ServicesCfg(dse), discovery.ChecksCfg(dck)); err != nil {
			return err
		}
	}
	return nil
}

// unregister Server from the service discovery system
func (s *Server) unregister() error {
	if _, ok := s.cfg.Discovery.(discovery.Discovery); ok {
		if err := s.cfg.Discovery.Unregister(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) healthHTTP() {
	r, err := http.Get(fmt.Sprintf(`http://localhost:%d/_health`, s.cfg.Port()))
	if err != nil {
		s.logger.Error().Msg(err.Error())
		s.health.SetServingStatus(s.cfg.ID, 2)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.logger.Error().Msg(err.Error())
		s.health.SetServingStatus(s.cfg.ID, 2)
		return
	}
	defer r.Body.Close()
	hcr := &healthpb.HealthCheckResponse{}
	if err := json.Unmarshal(b, hcr); err != nil {
		s.logger.Error().Msg(err.Error())
		s.health.SetServingStatus(s.cfg.ID, 2)
		return
	}
	s.health.SetServingStatus(s.cfg.ID, hcr.Status)
}

func (s *Server) healthGRPC() {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", s.cfg.Port()), grpc.WithInsecure())
	if err != nil {
		s.logger.Error().Msg(err.Error())
		s.health.SetServingStatus(s.cfg.ID, 2)
		return
	}
	defer conn.Close()
	c := healthpb.NewHealthClient(conn)
	hcr, err := c.Check(context.Background(), &healthpb.HealthCheckRequest{Service: s.cfg.ID})
	if err != nil {
		s.logger.Error().Msg(err.Error())
		s.health.SetServingStatus(s.cfg.ID, 2)
		return
	}
	s.health.SetServingStatus(s.cfg.ID, hcr.Status)
}

func (s *Server) setGRPCServer() {

	s.cfg.mu.Lock()
	// add default interceptors
	s.cfg.UnaryServerInterceptors = append(s.cfg.UnaryServerInterceptors,
		loggerUnaryServerInterceptor(s),
		serverUnaryServerInterceptor(s))

	s.cfg.StreamServerInterceptors = append(s.cfg.StreamServerInterceptors,
		loggerStreamServerInterceptor(s),
		serverStreamServerInterceptor(s))

	// initialize grpc server
	s.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(WrapperUnaryServer(s.cfg.UnaryServerInterceptors...)),
		grpc.StreamInterceptor(WrapperStreamServer(s.cfg.StreamServerInterceptors...)),
	)
	s.cfg.mu.Unlock()

	// register grpc internal health handlers
	healthpb.RegisterHealthServer(s.grpcServer, s.health)
	// register grpc handlers
	s.cfg.GRPCRegister(s.grpcServer)
}

// serveGRPC serves *grpc.Server
func (s *Server) serveGRPC(ln net.Listener) (err error) {

	// add go routine to WaitGroup
	s.wg.Add(1)
	go func(s *Server, ln net.Listener) {
		defer s.wg.Done()
		if err := s.grpcServer.Serve(ln); err != nil {
			if err.Error() == errClosing(ln).Error() {
				return
			}
			s.logger.Error().Msg(err.Error())
			return
		}
	}(s, ln)
	return nil
}
