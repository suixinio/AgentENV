package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	gateway "agentenv/services/gateway/internal"
	"agentenv/services/gateway/internal/resume"
	"agentenv/services/shared/config"
	"agentenv/services/shared/logging"
	"agentenv/services/shared/routing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newSchedulerConn(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	flag.Parse()

	cfg, err := config.Load(*configPath, "gateway")
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	logger, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logger.Sync()

	conn, err := newSchedulerConn(cfg.Gateway.SchedulerAddr)
	if err != nil {
		logger.Fatal("connect scheduler failed", zap.Error(err), zap.String("addr", cfg.Gateway.SchedulerAddr))
	}
	defer conn.Close()

	schedulerClient := schedulerv1.NewSchedulerClient(conn)
	queryOnlySchedulerClient := schedulerClient
	var queryOnlyConn *grpc.ClientConn
	if cfg.Gateway.QueryOnlySchedulerAddr != "" {
		queryOnlyConn, err = newSchedulerConn(cfg.Gateway.QueryOnlySchedulerAddr)
		if err != nil {
			logger.Fatal("connect query-only scheduler failed", zap.Error(err), zap.String("addr", cfg.Gateway.QueryOnlySchedulerAddr))
		}
		defer queryOnlyConn.Close()
		queryOnlySchedulerClient = schedulerv1.NewSchedulerClient(queryOnlyConn)
	}

	// 🔴 Built here and only when the read switch is on, so "the switch is off"
	// is a nil reader rather than a live connection nothing uses. NewReader
	// dials and pings, so a wrong address stops the process at start-up instead
	// of becoming a per-request warning that reads exactly like a cache miss.
	var projectionReader *routing.Reader
	if cfg.Gateway.Routing.ProjectionRead {
		projectionReader, err = routing.NewReader(cfg.Gateway.RedisAddr)
		if err != nil {
			logger.Fatal("connect routing projection redis failed", zap.Error(err), zap.String("addr", cfg.Gateway.RedisAddr))
		}
		defer projectionReader.Close()
	}

	// 🔴 Built only when an address is configured, following the projection
	// reader above: "the switch is off" has to be a nil client rather than a
	// live connection nothing uses. Unlike the projection reader this does not
	// dial here — grpc.NewClient is lazy — because an api half that is briefly
	// down must delay a wake-up, not stop the gateway from starting.
	var resumeClient *resume.Client
	if cfg.Gateway.ResumeAddr != "" {
		resumeConn, err := newSchedulerConn(cfg.Gateway.ResumeAddr)
		if err != nil {
			logger.Fatal("connect api resume surface failed", zap.Error(err), zap.String("addr", cfg.Gateway.ResumeAddr))
		}
		defer resumeConn.Close()
		resumeClient = resume.New(resumeConn, cfg.Gateway.RequestTimeout)
		logger.Info("waking paused sandboxes through the api half",
			zap.String("addr", cfg.Gateway.ResumeAddr),
		)
	} else {
		// 🔴 Said out loud, because the alternative is a capability that is
		// silently absent. With no address the gateway never asks anyone to
		// wake a sandbox: every projection miss goes to the scheduler and
		// whichever node the request lands on wakes it itself. That is correct
		// before 阶段 3a and wrong after it, and the difference is invisible
		// from the outside — the requests still succeed.
		logger.Info("no api resume surface configured; paused sandboxes are woken by the node the request lands on")
	}

	// 🔴 Said out loud in both positions, following the resume surface above and
	// for the same reason: with no address configured every user-facing REST
	// call is placed by the scheduler and served by a node, which is correct
	// before 阶段 3a and wrong after it, and the difference is invisible from
	// the outside because the calls succeed either way.
	if cfg.Gateway.RestUpstreamAddr != "" {
		logger.Info("sending user-facing rest to the api half",
			zap.String("addr", cfg.Gateway.RestUpstreamAddr),
		)
	} else {
		logger.Info("no api rest upstream configured; user-facing rest is served by the node the scheduler names")
	}

	serverOptions := gateway.ServerOptions{
		RequestTimeout:            cfg.Gateway.RequestTimeout,
		MaxResponseSize:           cfg.Gateway.ForwardResponseSize,
		DebugMode:                 cfg.Gateway.DebugMode,
		SandboxProxyDomains:       cfg.Gateway.SandboxProxyDomains,
		QueryOnlySchedulerClient:  queryOnlySchedulerClient,
		ExecutionFencing:          string(cfg.Gateway.Routing.ExecutionFencing),
		ControlPlaneToken:         cfg.Gateway.ControlPlaneToken,
		ProjectionAuthoritative:   cfg.Gateway.Routing.ProjectionAuthoritative,
		RestUpstreamAddr:          cfg.Gateway.RestUpstreamAddr,
		SchedulerFallbackDisabled: cfg.Gateway.SchedulerFallbackDisabled,
		SchedulerFallbackTimeout:  cfg.Gateway.SchedulerFallbackTimeout,
	}
	if cfg.Gateway.SchedulerFallbackDisabled {
		logger.Info("query-only scheduler fallback is disabled; a projection miss or an " +
			"undecided wake-up answers unavailable instead of asking the scheduler")
	}
	// 🔴 Assigned through the branch rather than passed inline: a typed nil
	// pointer stored in an interface field is not a nil interface, and the read
	// path checks the interface. Passing projectionReader unconditionally would
	// turn the switch off into a reader that panics on first use.
	if projectionReader != nil {
		serverOptions.ProjectionReader = projectionReader
	}
	serverOptions.ResumeClient = resumeClient

	s, err := gateway.NewServer(logger, schedulerClient, serverOptions)
	if err != nil {
		logger.Fatal("init gateway server failed", zap.Error(err))
	}

	logger.Info("gateway listening",
		zap.String("addr", cfg.Gateway.HTTPListenAddr),
		zap.String("metrics_addr", cfg.Gateway.MetricsListenAddr),
		zap.String("scheduler", cfg.Gateway.SchedulerAddr),
		zap.String("query_only_scheduler", cfg.Gateway.QueryOnlySchedulerAddr),
		zap.String("rest_upstream", cfg.Gateway.RestUpstreamAddr),
		zap.String("resume_addr", cfg.Gateway.ResumeAddr),
		zap.Strings("sandbox_proxy_domains", s.SandboxProxyDomains()),
		zap.String("execution_fencing", string(cfg.Gateway.Routing.ExecutionFencing)),
		zap.Bool("routing_projection_read", cfg.Gateway.Routing.ProjectionRead),
		zap.Bool("routing_projection_authoritative", cfg.Gateway.Routing.ProjectionAuthoritative),
		// Whether the token is set, never the token. An operator needs to know
		// which of the two states the gate is in, and that is the whole of it.
		zap.Bool("control_plane_token_configured", strings.TrimSpace(cfg.Gateway.ControlPlaneToken) != ""),
	)
	httpServer := &http.Server{
		Addr:    cfg.Gateway.HTTPListenAddr,
		Handler: s.Handler(),
	}
	metricsServer := &http.Server{
		Addr:    cfg.Gateway.MetricsListenAddr,
		Handler: promhttp.Handler(),
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("gateway serve failed", zap.Error(err))
		}
	}()
	go func() {
		logger.Info("gateway metrics server listening", zap.String("addr", cfg.Gateway.MetricsListenAddr))
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("gateway metrics serve failed", zap.Error(err))
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()

	httpShutdownCtx, cancelHTTPShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelHTTPShutdown()
	if err := httpServer.Shutdown(httpShutdownCtx); err != nil {
		logger.Warn("gateway graceful shutdown failed", zap.Error(err))
	}

	metricsShutdownCtx, cancelMetricsShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetricsShutdown()
	if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
		logger.Warn("gateway metrics graceful shutdown failed", zap.Error(err))
	}
}
