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
	"agentenv/services/scheduler/internal/catalog"
	pausedregistry "agentenv/services/scheduler/internal/registry"
	"agentenv/services/shared/config"
	"agentenv/services/shared/logging"

	"github.com/jackc/pgx/v5/pgxpool"
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

	// Reflects the configured *and* effective truth, not just the setting: a
	// cluster with the switch on but no write surface (no DSN, write disabled,
	// or a query-only replica) reads 0 here, the same as if it were off.
	scheduler.SetHeartbeatLeaseRenewalEnabled(registryWriter != nil && cfg.Scheduler.Registry.HeartbeatLeaseRenewal)

	// nil until the write surface is switched on; /healthz reports "off" then.
	var registryPhase func() (string, time.Duration, time.Duration)

	g := grpc.NewServer(grpc.UnaryInterceptor(scheduler.MetricsUnaryInterceptor()))
	if *queryOnly {
		// The registry goes to this replica too: a gateway pointed at a
		// query-only scheduler sends it every sandbox data-plane lookup, so a
		// registry wired only into the primary would never be consulted on the
		// path that needs it.
		queryOnlyOpts := []scheduler.QueryOnlyServiceOption{scheduler.WithQueryOnlyPausedRegistry(registryReader)}
		if cfg.Scheduler.Routing.ExecutionArbitration == config.SchedulerExecutionArbitrationOff {
			queryOnlyOpts = append(queryOnlyOpts, scheduler.WithQueryOnlySilentExecutionAxis())
		}
		svc := scheduler.NewQueryOnlyService(logger, store, queryOnlyOpts...)
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

		serviceOpts := []scheduler.ServiceOption{
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
		}
		if cfg.Scheduler.Routing.ExecutionArbitration == config.SchedulerExecutionArbitrationOff {
			serviceOpts = append(serviceOpts, scheduler.WithSilentExecutionAxis())
		}
		if cfg.Scheduler.Routing.ProjectionAuthoritative {
			serviceOpts = append(serviceOpts, scheduler.WithAuthoritativeProjection(cfg.Scheduler.MaxProjectionTTL))
		}
		if cfg.Scheduler.Routing.BindingSweep {
			serviceOpts = append(serviceOpts, scheduler.WithBindingSweep(cfg.Scheduler.BindingSweepSilence))
		}
		if registryWriter != nil {
			// registryWriter is a concrete *pausedregistry.PostgresStore here,
			// guarded non-nil before it is handed to the option as an
			// interface — checking after wrapping is the classic nil-interface
			// trap this guard exists to avoid.
			serviceOpts = append(serviceOpts, scheduler.WithHeartbeatLeaseRenewal(
				registryWriter, registryGrace, cfg.Scheduler.Registry.HeartbeatLeaseRenewal,
			))
		}
		announceBindingSweep(logger, cfg)
		svc := scheduler.NewService(
			logger,
			registry,
			scheduler.NewStrategy(cfg.Scheduler.Strategy),
			store,
			serviceOpts...,
		)
		go svc.RunObservedNodesMetrics(sigCtx, 15*time.Second)
		go svc.RunRegistryReconcile(sigCtx, cfg.Scheduler.Registry.ReconcileInterval)
		// Started unconditionally; the switch inside decides. See the note on
		// Service.RunBindingSweep for why the condition lives there and not
		// here.
		go svc.RunBindingSweep(sigCtx, cfg.Scheduler.BindingSweepInterval)
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

			// The snapshot catalog, on the registry's own pool.
			//
			// 🔴 The same pool and not a second one. A pause writes two facts —
			// the sandbox is parked, and here is the snapshot it parked into —
			// and the only reason the catalog is served from this process is
			// that those two can then be one transaction. A second pool would
			// be a second connection and the window would still be open.
			//
			// Registered before the migration has run and gated on the same
			// grace as the registry, which opens after both schemas are
			// applied. Until then every catalog RPC answers UNAVAILABLE — a
			// refusal a caller can act on, where an empty page would read as
			// "this cluster has no snapshots".
			catalogStore := catalog.NewStoreWithPool(registryWriter.Pool(),
				catalogStoreConfig(logger, cfg, scheduler.NewPausedHalfAdapter(registryWriter)))
			catalogSvc := scheduler.NewSnapshotCatalogService(
				logger, catalogStore, registryGrace, cfg.Scheduler.Registry.ClusterID,
			)
			schedulerv1.RegisterSnapshotCatalogServer(g, catalogSvc)

			// 🔴 Started here rather than inside the scoped branch below, and
			// not optional. `builds_one_active_per_template` turns one build
			// whose process died into a template nobody can ever build again,
			// so serving StartBuild without running this converts today's leak
			// into an outage for that template — which makes "the reaper runs
			// wherever the catalog is served" the invariant worth having, over
			// a condition somebody has to remember to mirror. Its own refusals
			// live in RunBuildReaper, which says so at error level and returns
			// rather than looping.
			go catalogSvc.RunBuildReaper(sigCtx,
				cfg.Scheduler.Catalog.BuildReapInterval,
				cfg.Scheduler.Catalog.BuildHeartbeatTTL,
			)
			announceBuildQueue(logger, cfg)

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
		// Both, together: "the projection is authoritative" is only true of a
		// store the other replicas can see, and an operator reading one of
		// these lines without the other would have half the answer.
		zap.Bool("projection_authoritative", cfg.Scheduler.Routing.ProjectionAuthoritative),
		zap.Duration("max_projection_ttl", cfg.Scheduler.MaxProjectionTTL),
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", registryHealthHandler(cfg.Scheduler.Registry.ClusterID, registryPhase, healthSwitches{
		writeFencing:         switchLabel(cfg.Scheduler.Registry.WriteFencing),
		executionArbitration: string(cfg.Scheduler.Routing.ExecutionArbitration),
	}))
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

// announceBindingSweep publishes the sweep's position and says the two things
// an operator cannot work out from the switch alone.
//
// 🔴 A query-only replica never reaches this. It has no node registry, no
// heartbeats and no discovery, so it has no standing to decide any node has
// gone — and the gauge stays at zero there, which is the truth about that
// process rather than a copy of the primary's setting.
// catalogStoreConfig is the catalog store's configuration, in one place a test
// can reach.
//
// 🔴 Extracted for the ceiling. A MaxConcurrentBuilds that never leaves the
// config is not an error anywhere: the store falls back to its own default and
// runs, so a cluster configured for four builds quietly admits twenty, and the
// only trace is the twenty-first refusal.
func catalogStoreConfig(logger *zap.Logger, cfg config.Config, paused catalog.PausedHalf) catalog.StoreConfig {
	return catalog.StoreConfig{
		Logger:              logger,
		Paused:              paused,
		MaxConcurrentBuilds: cfg.Scheduler.Catalog.MaxConcurrentBuilds,
	}
}

// announceBuildQueue says what the build queue's bounds resolved to, and warns
// about the one combination that is legal and still a bad idea.
//
// 🔴 Printed rather than left to be inferred, because these two numbers decide
// whether a user's build is refused and whether one that is running is thrown
// away, and neither answer is visible anywhere else until it happens.
func announceBuildQueue(logger *zap.Logger, cfg config.Config) {
	catalogCfg := cfg.Scheduler.Catalog
	fields := []zap.Field{
		zap.Int("max_concurrent_builds", catalogCfg.MaxConcurrentBuilds),
		zap.Duration("build_heartbeat_ttl", catalogCfg.BuildHeartbeatTTL),
		zap.Duration("build_reap_interval", catalogCfg.BuildReapInterval),
	}
	if catalogCfg.MaxConcurrentBuilds < 0 {
		// 🔴 Legal, and it takes writing a negative number, so it is a
		// deliberate choice. It is still worth one line saying what was chosen:
		// with no ceiling the only thing bounding concurrent builds is how many
		// VMs the fleet can boot, and the first symptom is nodes running out of
		// memory rather than a refusal anybody can read.
		logger.Warn("scheduler snapshot catalog build queue has no cluster-wide ceiling: nothing but the fleet's capacity limits how many builds run at once", fields...)
		return
	}
	logger.Info("scheduler snapshot catalog build queue enabled", fields...)
}

func announceBindingSweep(logger *zap.Logger, cfg config.Config) {
	enabled := cfg.Scheduler.Routing.BindingSweep
	silence := cfg.Scheduler.BindingSweepSilence
	scheduler.SetBindingSweep(enabled, silence)
	if !enabled {
		// 🔴 Warned about, not merely left at the default. With the write
		// switch on and no sweep, a node that dies hard leaves every record it
		// installed pointing at it for the record's full budget — up to
		// max_projection_ttl — and nothing on either read path cross-checks it.
		if cfg.Scheduler.Routing.ProjectionAuthoritative {
			logger.Warn("scheduler binding sweep is OFF while the routing projection is authoritative: a node that dies without unregistering strands every record it installed for up to max_projection_ttl",
				zap.String("setting", "scheduler.routing.binding_sweep"),
				zap.String("env", "SCHEDULER_ROUTING_BINDING_SWEEP"),
				zap.Duration("max_projection_ttl", cfg.Scheduler.MaxProjectionTTL),
			)
		}
		return
	}

	// 🔴 The one ordering the sweep depends on. The roster fallback answers for
	// any node whose last heartbeat is within report_ttl, so a threshold at or
	// below it retires a record the fallback immediately re-answers with the
	// same dead node: the sweep becomes a no-op that also costs a lookup.
	if silence <= cfg.Scheduler.ReportTTL {
		logger.Warn("scheduler binding sweep threshold is not above the report TTL, so the heartbeat roster will keep answering for nodes whose records it retires",
			zap.Duration("binding_sweep_silence", silence),
			zap.Duration("report_ttl", cfg.Scheduler.ReportTTL),
			zap.String("env", "SCHEDULER_BINDING_SWEEP_SILENCE"),
		)
	}
	if !cfg.Scheduler.Routing.ProjectionAuthoritative {
		logger.Info("scheduler binding sweep is on while the routing projection is ephemeral: records expire on binding_ttl on their own, so the sweep will rarely find one left to retire",
			zap.Duration("binding_ttl", cfg.Scheduler.BindingTTL),
		)
	}
	logger.Info("scheduler binding sweep enabled",
		zap.Duration("silence_threshold", silence),
		zap.Duration("interval", cfg.Scheduler.BindingSweepInterval),
	)
}

func createBindingStore(logger *zap.Logger, cfg config.Config) (scheduler.BindingStore, func()) {
	mode := cfg.Scheduler.Routing.ExecutionArbitration

	// 🔴 Loud when it is not enforcing, and resident in a gauge as well as in
	// this line: the failure of a scheduler that is not arbitrating is a
	// sandbox whose traffic goes to the wrong copy, which nothing in the logs
	// of the moment will say.
	scheduler.SetRoutingExecutionArbitration(string(mode))
	scheduler.SetBindingArbitrationLogger(logger)
	if mode == config.SchedulerExecutionArbitrationOff {
		logger.Warn("scheduler binding arbitration is OFF: whichever node reports last owns a sandbox's binding, so a superseded incarnation takes it back on every heartbeat",
			zap.String("setting", "scheduler.routing.execution_arbitration"),
			zap.String("env", "SCHEDULER_ROUTING_EXECUTION_ARBITRATION"),
		)
	}

	// 🔴 The same setting the Service reads, handed to the store as well. The
	// Service decides whether a node's budget is honoured; the store decides
	// whether a heartbeat may leave an existing deadline alone. Wiring only the
	// first is how the switch shipped able to lengthen records but not to
	// shorten them again.
	projectionAuthoritative := cfg.Scheduler.Routing.ProjectionAuthoritative

	if strings.TrimSpace(cfg.Scheduler.RedisAddr) == "" {
		return scheduler.NewInMemoryBindingStoreWithModes(
			cfg.Scheduler.BindingTTL,
			scheduler.InMemoryArbitrationFor(string(mode)),
			projectionAuthoritative), func() {}
	}

	store, err := scheduler.NewRedisBindingStoreWithModes(
		cfg.Scheduler.RedisAddr,
		cfg.Scheduler.BindingTTL,
		scheduler.RedisArbitrationFor(string(mode)),
		projectionAuthoritative)
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

// healthSwitches is what /healthz says about the two settings that can turn a
// half of this release off. Both are reported by name and by value, never as a
// bare boolean somewhere in the body: an operator reading this during an
// incident has to be able to tell "off" from "absent from this build".
type healthSwitches struct {
	writeFencing         string
	executionArbitration string
}

func switchLabel(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
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
		// 🔴 Both gauges are set here, on the path that builds nothing, and
		// that is the point of this pair. Leaving them at the default made
		// "fencing is off" and "there is no write surface here" the same
		// reading — every query-only replica and every write_enabled=false
		// cluster published a 0 that an alerting rule could not tell from the
		// one shape that needs an operator. See registryWriteSurfaceEnabled for
		// how the three registry gauges read as a tuple.
		scheduler.SetRegistryWriteSurfaceEnabled(false)
		scheduler.SetRegistryWriteFencingEnabled(false)
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
		WriteFencing:   cfg.Scheduler.Registry.WriteFencing,
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

	// 🔴 Loud when it is off. This is the half whose failure costs a
	// workspace, and the way an operator finds out today would otherwise be a
	// stale snapshot published over a live one, weeks later, with nothing
	// pointing back to a setting.
	//
	// The surface gauge goes with it, always, so the fencing one is only ever
	// read where it means something. 1 here is "assembled", not "serving": a
	// surface with no cluster id is registered and stays cold, and only
	// /healthz's phase can say that.
	scheduler.SetRegistryWriteSurfaceEnabled(true)
	scheduler.SetRegistryWriteFencingEnabled(cfg.Scheduler.Registry.WriteFencing)
	if !cfg.Scheduler.Registry.WriteFencing {
		logger.Warn("paused registry write fencing is DISABLED: an incarnation the cluster has written off can overwrite the row of the one that replaced it",
			zap.String("setting", "scheduler.registry.write_fencing"),
			zap.String("env", "SCHEDULER_REGISTRY_WRITE_FENCING"),
		)
	}

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
			err = migrateCatalog(ctx, store)
		}
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

		logger.Error("paused registry and snapshot catalog write surfaces are not open; every request to either is refused until they are",
			zap.Error(err),
			zap.Duration("retry_in", backoff),
		)
		if !sleepWithContext(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

// migrateCatalog applies the snapshot catalog's schema, on the same pool and in
// the same retry loop as the registry's.
//
// 🔴 Same loop, and after the registry's migration rather than beside it. Both
// take the same advisory lock, so running them concurrently from one process
// would be a lock this process waits on itself for; running them in a fixed
// order makes that impossible.
//
// 🔴 What a failure here costs, stated exactly, because half of it is easy to
// miss. The gate is shared: an error returned from this function means
// grace.Enter is never reached, the phase stays cold, and Grace.Require refuses
// — so it is not only the catalog's own RPCs that answer UNAVAILABLE. Every
// paused-registry *write* does too: begin_pause, complete_pause,
// mark_local_only, claim_for_resume. That is phase 1's live functionality, held
// shut by a schema this process had never applied before, and it is the reason
// the migration is worth being careful with rather than a background detail.
//
// Routing, discovery and bindings do keep working, which is why this is
// retried rather than fatal — but "leaves the rest of the process alone" is
// three subsystems, not all of them. TestACatalogMigrationFailureHoldsThe
// PausedRegistryShut in this package is what keeps that from drifting back
// into a comfortable half-truth.
//
// A store that is not the postgres one has no pool and no catalog to migrate.
// NewStore only ever returns the postgres one today, so this is the shape of a
// future in-memory store rather than a case that happens.
func migrateCatalog(ctx context.Context, store pausedregistry.Store) error {
	pooled, ok := store.(interface{ Pool() *pgxpool.Pool })
	if !ok {
		return nil
	}
	return catalog.Migrate(ctx, pooled.Pool())
}

// registryHealthHandler reports the write surface's phase, and the cluster it
// is scoped to.
//
// 🔴 Always 200, deliberately. This says something about one subsystem, and a
// probe wired to it that took the pod out of rotation would answer a registry
// database outage by also stopping the routing and discovery that had nothing
// to do with it. The phase is in the body for whoever is looking; nothing here
// gates the process.
//
// 🔴 The cluster id is here because "cold" has two causes that look identical
// from outside — the database is unreachable, or nothing ever supplied a
// cluster scope — and the second one leaves a process that is healthy by every
// other measure: /healthz 200, gRPC probe passing, one error line at startup
// that has long since scrolled away. An operator who can read the id back can
// tell "configured" from "assumed" without redeploying anything.
func registryHealthHandler(clusterID string, phase func() (string, time.Duration, time.Duration), switches healthSwitches) http.HandlerFunc {
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
				"cluster_id":                strings.TrimSpace(clusterID),
				"grace_remaining_seconds":   remaining.Seconds(),
				"inferred_downtime_seconds": downtime.Seconds(),
				// 🔴 Reported next to the phase because "the registry is
				// healthy" and "the registry is checking who is writing to it"
				// are separate questions and only one of them has ever been
				// visible here.
				"write_fencing": switches.writeFencing,
			}
		}
		body["routing"] = map[string]any{
			"execution_arbitration": switches.executionArbitration,
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
