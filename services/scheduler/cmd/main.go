package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	scheduler "agentenv/services/scheduler/internal"
	pausedregistry "agentenv/services/scheduler/internal/registry"
	"agentenv/services/shared/config"
	"agentenv/services/shared/logging"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/client-go/rest"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	queryOnly := flag.Bool("query-only", false, "run a query-only scheduler that supports only LookupNode; requires scheduler.redis_addr")
	flag.Parse()

	cfg, err := config.LoadScheduler(*configPath, *queryOnly)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	logger, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logger.Sync()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, closeStore := createBindingStore(logger, cfg)
	defer closeStore()

	registryReader, closeRegistry := createRegistryReader(logger, cfg)
	defer closeRegistry()

	g := grpc.NewServer(grpc.UnaryInterceptor(scheduler.MetricsUnaryInterceptor()))
	if *queryOnly {
		// The registry goes to this replica too: a gateway pointed at a
		// query-only scheduler sends it every sandbox data-plane lookup, so a
		// registry wired only into the primary would never be consulted on the
		// path that needs it.
		svc := scheduler.NewQueryOnlyService(logger, store, scheduler.WithQueryOnlyPausedRegistry(registryReader))
		schedulerv1.RegisterSchedulerServer(g, svc)
		logger.Info("scheduler query-only service enabled", zap.String("redis_addr", cfg.Scheduler.RedisAddr))
	} else {
		registry := scheduler.NewAtomicNodeRegistry(nil, cfg.Scheduler.ReportTTL)
		switch strings.ToLower(strings.TrimSpace(cfg.Scheduler.Discovery.Mode)) {
		case "kubernetes":
			go runKubernetesDiscoveryWithRetry(sigCtx, logger, cfg.Scheduler.Discovery.Kubernetes, registry)
		default:
			nodes := make([]scheduler.Node, 0, len(cfg.Scheduler.Nodes))
			for _, n := range cfg.Scheduler.Nodes {
				nodes = append(nodes, scheduler.Node{ID: n.ID, Endpoint: n.Endpoint})
			}
			registry.Set(nodes, nil)
		}

		svc := scheduler.NewService(
			logger,
			registry,
			scheduler.NewStrategy(cfg.Scheduler.Strategy),
			store,
			scheduler.WithArtifactStore(scheduler.NewInMemoryArtifactStore(
				cfg.Scheduler.ArtifactStoreCapacity,
				cfg.Scheduler.ArtifactLookupNodeLimit,
			)),
			scheduler.WithNodeResourceLimit(cfg.Scheduler.NodeResourceLimit),
			scheduler.WithWarmupTimeout(cfg.Scheduler.WarmupTimeout),
			scheduler.WithPausedRegistry(
				registryReader,
				cfg.Scheduler.ReportTTL,
				cfg.Scheduler.Registry.LeaseWarnWindow,
			),
		)
		go svc.RunObservedNodesMetrics(sigCtx, 15*time.Second)
		go svc.RunRegistryReconcile(sigCtx, cfg.Scheduler.Registry.ReconcileInterval)
		schedulerv1.RegisterSchedulerServer(g, svc)
	}

	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus(schedulerv1.Scheduler_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(g, hs)

	lis, err := net.Listen("tcp", cfg.Scheduler.GRPCListenAddr)
	if err != nil {
		logger.Fatal("listen failed", zap.Error(err), zap.String("addr", cfg.Scheduler.GRPCListenAddr))
	}
	logger.Info("scheduler gRPC server listening",
		zap.String("addr", cfg.Scheduler.GRPCListenAddr),
		zap.String("strategy", cfg.Scheduler.Strategy),
		zap.String("binding_store", bindingStoreName(cfg)),
		zap.Bool("query_only", *queryOnly),
		zap.Bool("paused_registry", registryEnabled(cfg)),
	)

	metricsServer := &http.Server{
		Addr:    cfg.Scheduler.MetricsListenAddr,
		Handler: promhttp.Handler(),
	}
	go func() {
		logger.Info("scheduler metrics server listening", zap.String("addr", metricsServer.Addr))
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("scheduler metrics serve failed", zap.Error(err))
		}
	}()

	serveErrCh := make(chan error, 1)
	go func() {
		err := g.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErrCh <- err
			return
		}
		serveErrCh <- nil
	}()

	select {
	case err := <-serveErrCh:
		if err != nil {
			logger.Fatal("serve failed", zap.Error(err))
		}
		return
	case <-sigCtx.Done():
	}

	logger.Info("scheduler shutdown signal received")
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	hs.SetServingStatus(schedulerv1.Scheduler_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	gracefulStopDone := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(gracefulStopDone)
	}()

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case <-gracefulStopDone:
		logger.Info("scheduler stopped gracefully")
	case <-timer.C:
		logger.Warn("scheduler graceful shutdown timed out; forcing stop")
		g.Stop()
		<-gracefulStopDone
	}

	metricsShutdownCtx, cancelMetricsShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetricsShutdown()
	if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
		logger.Warn("scheduler metrics graceful shutdown failed", zap.Error(err))
	}

	if err := <-serveErrCh; err != nil {
		logger.Fatal("serve failed", zap.Error(err))
	}
}

func createBindingStore(logger *zap.Logger, cfg config.Config) (scheduler.BindingStore, func()) {
	if strings.TrimSpace(cfg.Scheduler.RedisAddr) == "" {
		return scheduler.NewInMemoryBindingStore(cfg.Scheduler.BindingTTL), func() {}
	}

	store, err := scheduler.NewRedisBindingStore(cfg.Scheduler.RedisAddr, cfg.Scheduler.BindingTTL)
	if err != nil {
		logger.Fatal("create redis binding store failed", zap.Error(err), zap.String("addr", cfg.Scheduler.RedisAddr))
	}
	return store, func() {
		if err := store.Close(); err != nil {
			logger.Warn("close redis binding store failed", zap.Error(err))
		}
	}
}

// createRegistryReader builds the read-only paused-registry reader, or the
// disabled one when no DSN is configured.
//
// It does not connect. A registry database that is down must not stop the
// scheduler from starting: routing, discovery, and bindings all work without
// it, and refusing to start would turn an observability outage into a cluster
// outage. Only a DSN that cannot be parsed is fatal, and that is a
// configuration error that will not fix itself.
func createRegistryReader(logger *zap.Logger, cfg config.Config) (pausedregistry.Reader, func()) {
	scheduler.SetRegistryEnabled(registryEnabled(cfg))
	if !registryEnabled(cfg) {
		return pausedregistry.Disabled(), func() {}
	}

	if strings.TrimSpace(cfg.Scheduler.Registry.ClusterID) == "" {
		// Fail-open, and loudly. The cluster id arrives from an optional Secret
		// key, so a missing key or a typo in its name leaves it empty and every
		// read silently widens to the whole database — which is the accident
		// where one cluster reconciles another's rows. Reading nothing would be
		// worse for a single-cluster database, which is the common case, so the
		// scheduler carries on and says so.
		logger.Warn("scheduler paused registry has no cluster id; every read covers every cluster in the database",
			zap.String("env", "SCHEDULER_REGISTRY_CLUSTER_ID"),
		)
	}

	reader, err := pausedregistry.New(context.Background(), pausedregistry.Config{
		DSN:            cfg.Scheduler.Registry.DSN,
		ClusterID:      cfg.Scheduler.Registry.ClusterID,
		MaxConnections: cfg.Scheduler.Registry.MaxConnections,
		QueryTimeout:   cfg.Scheduler.Registry.QueryTimeout,
	})
	if err != nil {
		logger.Fatal("create paused registry reader failed", zap.Error(err))
	}

	logger.Info("scheduler paused registry enabled",
		zap.String("cluster_id", cfg.Scheduler.Registry.ClusterID),
		zap.Int32("max_connections", cfg.Scheduler.Registry.MaxConnections),
		zap.Duration("reconcile_interval", cfg.Scheduler.Registry.ReconcileInterval),
	)
	return reader, reader.Close
}

func registryEnabled(cfg config.Config) bool {
	return strings.TrimSpace(cfg.Scheduler.Registry.DSN) != ""
}

func bindingStoreName(cfg config.Config) string {
	if strings.TrimSpace(cfg.Scheduler.RedisAddr) != "" {
		return "redis"
	}
	return "memory"
}

func runKubernetesDiscoveryWithRetry(
	ctx context.Context,
	logger *zap.Logger,
	cfg config.SchedulerDiscoveryKubernetesConfig,
	registry *scheduler.AtomicNodeRegistry,
) {
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	attempt := 0

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		attempt++
		discovery, err := scheduler.NewKubernetesDiscovery(logger, cfg, registry)
		if err != nil {
			if errors.Is(err, rest.ErrNotInCluster) {
				logger.Error("kubernetes discovery initialization failed with non-retryable error; stopping discovery loop",
					zap.Error(err),
					zap.Int("attempt", attempt),
				)
				return
			}

			logger.Warn("kubernetes discovery initialization failed; retrying",
				zap.Error(err),
				zap.Int("attempt", attempt),
				zap.Duration("retry_in", backoff),
			)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		err = discovery.Run(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}

		logger.Warn("kubernetes discovery stopped unexpectedly; retrying",
			zap.Error(err),
			zap.Int("attempt", attempt),
			zap.Duration("retry_in", backoff),
		)
		if !sleepWithContext(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current time.Duration, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
