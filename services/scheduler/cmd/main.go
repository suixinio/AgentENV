package main

import (
	"context"
	"encoding/json"
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

	// 🔴 Never on a query-only replica. Those exist so sandbox lookups survive a
	// primary restart; there is exactly one owner of this table's shape and of
	// the reclamation timer that deletes rows from it, and a replica that
	// migrated and reclaimed alongside the primary would be a second one.
	registryWriter, registryGrace, closeRegistryWriter := createRegistryStore(logger, cfg, *queryOnly)
	defer closeRegistryWriter()

	// nil until the write surface is switched on; /healthz reports "off" then.
	var registryPhase func() (string, time.Duration, time.Duration)

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

		if registryWriter != nil {
			// Registered before the migration has run, on purpose. The service
			// answers UNAVAILABLE until the gate opens, which is the one thing
			// it must do — a registry that is not ready has to say so, because
			// the alternative shape of "not ready" is an empty answer, and the
			// node deletes a workspace on the strength of one of those.
			registrySvc := scheduler.NewPausedRegistryService(
				logger, registryWriter, registryGrace,
				cfg.Scheduler.Registry.ClusterID,
				cfg.Scheduler.Registry.LeaseTTL,
				cfg.Scheduler.Registry.LeaseTTLFloor,
			)
			schedulerv1.RegisterPausedRegistryServer(g, registrySvc)
			registryPhase = registrySvc.Phase

			// 🔴 Registered either way, and left cold when there is no cluster
			// scope. Not registering would answer Unimplemented, which reads as
			// "this build does not have the feature" rather than "it is
			// configured and not usable"; and the phase this leaves behind is
			// what /healthz and the metric report, so the operator sees one
			// story instead of two.
			if !registryWriteScoped(cfg) {
				logger.Error("scheduler paused registry write surface is configured but has no cluster id, "+
					"so it will stay cold: the restart grace pass and the reclamation timer will not run, "+
					"and every registry RPC will be answered UNAVAILABLE until a cluster id is set",
					zap.String("env", "SCHEDULER_REGISTRY_CLUSTER_ID"),
				)
			} else {
				go openRegistryWriteSurface(sigCtx, logger, cfg, registryWriter, registryGrace, registryWriter)
				go registrySvc.RunReclaim(sigCtx, cfg.Scheduler.Registry.ReclaimInterval)
				logger.Info("scheduler paused registry write surface enabled",
					zap.String("cluster_id", cfg.Scheduler.Registry.ClusterID),
					zap.Duration("lease_ttl", cfg.Scheduler.Registry.LeaseTTL),
					zap.Duration("lease_ttl_floor", cfg.Scheduler.Registry.LeaseTTLFloor),
					zap.Duration("reclaim_interval", cfg.Scheduler.Registry.ReclaimInterval),
				)
			}
		}
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

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", registryHealthHandler(registryPhase))
	metricsServer := &http.Server{
		Addr:    cfg.Scheduler.MetricsListenAddr,
		Handler: mux,
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

// createRegistryStore builds the writable store, or nothing when the write
// surface is switched off.
//
// It does not connect and it does not migrate: see openRegistryWriteSurface.
//
// The concrete type is returned rather than the Store interface because the
// restart grace pass is not part of that interface: it is this process's own
// repair of its own absence, not an operation any node can ask for.
func createRegistryStore(logger *zap.Logger, cfg config.Config, queryOnly bool) (*pausedregistry.PostgresStore, *pausedregistry.Grace, func()) {
	if queryOnly && registryWriteEnabled(cfg) {
		logger.Info("scheduler paused registry write surface stays off on a query-only replica")
	}
	if queryOnly || !registryWriteEnabled(cfg) {
		return nil, nil, func() {}
	}

	grace := pausedregistry.NewGrace(cfg.Scheduler.Registry.LeaseTTL, logger)
	breaker := pausedregistry.NewDiscardBreaker(
		cfg.Scheduler.Registry.DiscardMaxRows,
		cfg.Scheduler.Registry.DiscardMaxRatio,
		logger,
	)

	built, err := pausedregistry.NewStore(context.Background(), pausedregistry.StoreConfig{
		DSN:            cfg.Scheduler.Registry.DSN,
		Logger:         logger,
		LeaseTTL:       cfg.Scheduler.Registry.LeaseTTL,
		MaxConnections: cfg.Scheduler.Registry.WriteMaxConnections,
		QueryTimeout:   cfg.Scheduler.Registry.QueryTimeout,
	})
	if err != nil {
		// A DSN that will not parse is a configuration error that does not fix
		// itself, and this one is the credentials for the table this process is
		// being asked to own.
		logger.Fatal("create paused registry store failed", zap.Error(err))
	}

	// The restart gate and the discard breaker are this process's own, not part
	// of the Store seam the nodes see, so they are attached to the concrete
	// type. NewStore only ever returns this one.
	store, ok := built.(*pausedregistry.PostgresStore)
	if !ok {
		logger.Fatal("paused registry store is not the postgres implementation; the restart gate cannot be attached")
	}
	store.WithGuards(grace, breaker)

	return store, grace, store.Close
}

// openRegistryWriteSurface migrates, runs the restart grace pass, and opens the
// gate — retrying until it succeeds or the process is shutting down.
//
// 🔴 Retried rather than fatal. This process routes traffic, discovers nodes
// and serves bindings, all of which work with the registry database on fire;
// exiting because it is would turn one subsystem's outage into the cluster's.
// The gate is what makes that safe: until this returns, every registry request
// is answered UNAVAILABLE rather than with an empty result.
func openRegistryWriteSurface(
	ctx context.Context,
	logger *zap.Logger,
	cfg config.Config,
	store pausedregistry.Store,
	grace *pausedregistry.Grace,
	extender pausedregistry.LeaseExtender,
) {
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return
		}

		err := store.Migrate(ctx)
		if err == nil {
			var observed pausedregistry.GraceObservation
			observed, err = grace.Enter(ctx, extender, cfg.Scheduler.Registry.ClusterID)
			if err == nil {
				logger.Info("paused registry write surface open",
					zap.Duration("inferred_downtime", observed.Downtime),
					zap.Int64("leases_extended", observed.Extended),
					zap.Time("grace_until", observed.Until),
				)
				return
			}
		}

		logger.Error("paused registry write surface is not open; every registry request is refused until it is",
			zap.Error(err),
			zap.Duration("retry_in", backoff),
		)
		if !sleepWithContext(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

// registryHealthHandler reports the write surface's phase.
//
// 🔴 Always 200, deliberately. This says something about one subsystem, and a
// probe wired to it that took the pod out of rotation would answer a registry
// database outage by also stopping the routing and discovery that had nothing
// to do with it. The phase is in the body for whoever is looking; nothing here
// gates the process.
func registryHealthHandler(phase func() (string, time.Duration, time.Duration)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := map[string]any{"status": "ok"}
		if phase == nil {
			body["registry_write"] = map[string]any{"phase": "off"}
		} else {
			name, remaining, downtime := phase()
			body["registry_write"] = map[string]any{
				"phase":                     name,
				"ready":                     name != "cold",
				"serving":                   name == "serving",
				"grace_remaining_seconds":   remaining.Seconds(),
				"inferred_downtime_seconds": downtime.Seconds(),
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(body)
	}
}

func registryWriteEnabled(cfg config.Config) bool {
	return registryEnabled(cfg) && cfg.Scheduler.Registry.WriteEnabled
}

// registryWriteScoped reports whether the write surface has everything it needs
// to actually open.
//
// 🔴 Separate from registryWriteEnabled because a missing cluster id leaves the
// surface *registered and cold* rather than absent: it was configured on, so
// reporting it as "off" would be a lie, and answering Unimplemented would read
// as "this build does not have the feature". Cold answers UNAVAILABLE, which is
// both true and the one shape a node must never mistake for "the cluster knows
// of no such sandbox".
func registryWriteScoped(cfg config.Config) bool {
	return registryWriteEnabled(cfg) && strings.TrimSpace(cfg.Scheduler.Registry.ClusterID) != ""
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
