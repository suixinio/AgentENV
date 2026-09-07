//! Brokered egress against a real `aenv-egress` process on this machine: the
//! guest's 443 traffic is intercepted in its namespace, crosses the node-local
//! Unix socket with the right identity and original destination, and the
//! listeners follow the policy through updates, pause/resume and slot reuse.
//!
//! A `rules`-derived policy names `echo` here where a public policy names
//! `http`, because the broker these tests start has no credential source;
//! everything between the guest and the broker is the production path.
//! Requires root, `/dev/kvm`, `openssl`, a node config with
//! `[egress_broker].mode = "local"` and a `socket_path`, and a broker binary
//! at `AENV_EGRESS_BINARY_PATH` — `make test-agent-integration` supplies all
//! three. Passthrough of unmatched SNI and policy denial at the broker need a
//! credential source and are covered when it lands.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, OnceLock};

use aenv_node::cfg::{ConfigManager, EgressBrokerMode};
use aenv_node::sandbox::network::policy::{DomainRule, EndpointDeclaration, HeaderTransform};
use aenv_node::sandbox::{
    BaseSandboxNetworkPolicy, FirecrackerSandbox, SandboxBackend, SandboxExecutor,
    SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
};
use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::common;

const FAKE_UPSTREAM: &str = "203.0.113.10";
/// Where a brokered listener binds inside every sandbox namespace: the tap
/// side of the VM link, from `network.internal`'s fixed VM link CIDR.
const BROKER_LISTENER_IP: &str = "169.254.0.22";
const ENDPOINT_PORT: u16 = 15432;

/// Asks the owner thread for a broker that is running and reachable, and
/// refuses to run against any other broker mode.
///
/// Every test calls this, and every test runs on its own thread. The child is
/// therefore never spawned here: `PR_SET_PDEATHSIG` names the *thread* that
/// spawned it, so a broker started from a test's own thread is signalled the
/// moment that test returns, and every later test finds a socket that is
/// gone. It is spawned by [`broker_owner`]'s thread instead, which lives as
/// long as the process does.
fn require_local_broker() -> Result<()> {
    let egress = &ConfigManager::global_config().egress_broker;
    if egress.mode != EgressBrokerMode::Local {
        bail!(
            "these tests need [egress_broker].mode = \"local\" in the node config; it is {}",
            egress.mode.as_str()
        );
    }
    let socket_path = egress
        .socket_path
        .clone()
        .context("[egress_broker].socket_path is required in local mode")?;

    let (reply, answer) = mpsc::channel();
    broker_owner()
        .send(EnsureRunning { socket_path, reply })
        .context("the broker owner thread is gone")?;
    answer
        .recv()
        .context("the broker owner thread answered nothing")?
}

/// What the owner thread is asked for: a broker on this socket, running now.
struct EnsureRunning {
    socket_path: PathBuf,
    reply: mpsc::Sender<Result<()>>,
}

/// The one thread that spawns and holds the broker child.
///
/// The sender is a `static`, so the channel never closes and the loop never
/// ends: the thread outlives every test and dies with the process, which is
/// exactly when the broker should die with it.
fn broker_owner() -> &'static mpsc::Sender<EnsureRunning> {
    static OWNER: OnceLock<mpsc::Sender<EnsureRunning>> = OnceLock::new();
    OWNER.get_or_init(|| {
        let (requests, incoming) = mpsc::channel::<EnsureRunning>();
        std::thread::Builder::new()
            .name("aenv-egress-owner".to_string())
            .spawn(move || {
                let mut child: Option<Child> = None;
                for request in incoming {
                    let answer = ensure_running(&mut child, &request.socket_path);
                    let _ = request.reply.send(answer);
                }
            })
            .expect("spawn the broker owner thread");
        requests
    })
}

/// Runs on the owner thread. A child that exited, or one whose socket no
/// longer accepts, is replaced rather than reported as running.
fn ensure_running(child: &mut Option<Child>, socket_path: &Path) -> Result<()> {
    if let Some(running) = child.as_mut() {
        let exited = running.try_wait().ok().flatten().is_some();
        if !exited && std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
            return Ok(());
        }
        if !exited {
            let _ = running.kill();
        }
        let _ = running.wait();
        *child = None;
    }
    *child = Some(start_broker(socket_path)?);
    Ok(())
}

fn start_broker(socket_path: &Path) -> Result<Child> {
    let binary = std::env::var_os("AENV_EGRESS_BINARY_PATH")
        .map(PathBuf::from)
        .context("AENV_EGRESS_BINARY_PATH must name an aenv-egress built with --features bin")?;
    let directory = socket_path
        .parent()
        .context("the broker socket path names no directory")?;
    std::fs::create_dir_all(directory)
        .with_context(|| format!("create the broker socket directory {directory:?}"))?;
    let _ = std::fs::remove_file(socket_path);

    let (ca_cert, ca_key) = mint_broker_ca(directory)?;
    let config_path = directory.join("aenv-egress.toml");
    let mut config = std::fs::File::create(&config_path)
        .with_context(|| format!("write the broker config {config_path:?}"))?;
    write!(
        config,
        "admission_timeout_ms = 10000\n\
         max_connections = 256\n\
         per_sandbox_connections = 64\n\
         shutdown_drain_secs = 2\n\
         \n\
         [listen]\n\
         socket_path = {socket_path:?}\n\
         peer_uid = {uid}\n\
         \n\
         [ca]\n\
         cert_path = {ca_cert:?}\n\
         key_path = {ca_key:?}\n\
         \n\
         [handlers]\n\
         echo = true\n",
        uid = unsafe { libc::getuid() },
    )?;
    drop(config);

    let mut command = Command::new(&binary);
    command
        .arg("--config")
        .arg(&config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    // SAFETY: `prctl` here only arms a signal for this child; the closure
    // allocates nothing and calls nothing that is not async-signal-safe.
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(&mut command, || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command
        .spawn()
        .with_context(|| format!("spawn the broker at {binary:?}"))?;

    for _ in 0..100 {
        if std::os::unix::net::UnixStream::connect(socket_path).is_ok() {
            return Ok(child);
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    bail!("the broker never bound {socket_path:?}");
}

/// A throwaway CA for the broker to sign intercepted names with. No guest in
/// these tests terminates TLS, so nothing has to trust it; the broker refuses
/// to start without one.
fn mint_broker_ca(directory: &Path) -> Result<(PathBuf, PathBuf)> {
    let cert = directory.join("ca.crt");
    let key = directory.join("ca.key");
    if cert.exists() && key.exists() {
        return Ok((cert, key));
    }
    run_openssl(
        &[
            "ecparam",
            "-name",
            "prime256v1",
            "-genkey",
            "-noout",
            "-out",
        ],
        &key,
    )?;
    let status = Command::new("openssl")
        .args(["req", "-x509", "-new", "-key"])
        .arg(&key)
        .args([
            "-sha256",
            "-days",
            "1",
            "-subj",
            "/CN=AgentENV Integration Egress CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-out",
        ])
        .arg(&cert)
        .status()
        .context("run openssl req; these tests need the openssl CLI")?;
    if !status.success() {
        bail!("openssl req failed: {status}");
    }
    Ok((cert, key))
}

fn run_openssl(args: &[&str], out: &Path) -> Result<()> {
    let status = Command::new("openssl")
        .args(args)
        .arg(out)
        .status()
        .context("run openssl; these tests need the openssl CLI")?;
    if !status.success() {
        bail!("openssl {args:?} failed: {status}");
    }
    Ok(())
}

fn policy_with_rules(
    base: BaseSandboxNetworkPolicy,
    deny_out: Option<Vec<String>>,
) -> Result<SandboxNetworkPolicy> {
    let mut rules = std::collections::BTreeMap::new();
    rules.insert(
        "fake.test".to_string(),
        vec![DomainRule {
            transform: HeaderTransform {
                headers: [(
                    "Authorization".to_string(),
                    "Bearer ${aenv.secrets.fake}".to_string(),
                )]
                .into_iter()
                .collect(),
            },
        }],
    );
    let mut policy = SandboxNetworkPolicy::new(
        base,
        SandboxNetworkEgressPolicy::with_rules(None, deny_out, Some(rules))?,
    );
    // `with_rules` names the `http` handler, which needs a credential source;
    // the broker these tests start has none and would answer 502 on the first
    // marker, so they relay through `echo` instead.
    for broker in &mut policy.egress.brokers {
        broker.handler = "echo".to_string();
    }
    Ok(policy)
}

/// Connects to `dest:443` from the guest, reads the identity banner the
/// `echo` handler answers with, then proves the echo path.
async fn brokered_banner(sandbox: &mut FirecrackerSandbox, dest: &str) -> Result<Option<Value>> {
    banner_on_port(sandbox, dest, 443).await
}

/// The same, for a port an explicit endpoint declared.
///
/// `None` means the intercept is not there, and that is decided by whether a
/// banner came back — not by how the connection failed. `FAKE_UPSTREAM` is a
/// documentation address nothing answers on, so an uninterrupted attempt is
/// refused on a machine with no route to it (`exit 7`) and times out on one
/// that has an egress path (`exit 124`); a harness that only understood the
/// first read the second as a broken test.
async fn banner_on_port(
    sandbox: &mut FirecrackerSandbox,
    dest: &str,
    port: u16,
) -> Result<Option<Value>> {
    // stderr is not merged in: a refused connection makes bash print
    // `connect: Connection refused` on it, and a line on stdout is how this
    // says "a banner came back". Merging them turned the refusal — one of the
    // two ways "no intercept" looks — into a parse error.
    let script = format!(
        "timeout 5 bash -c 'exec 3<>/dev/tcp/{dest}/{port} || exit 7; \
         IFS= read -r line <&3; printf \"%s\\n\" \"$line\"; \
         printf ping >&3; exec 3>&-; ' 2>/dev/null"
    );
    let output = sandbox.run_command("bash", &["-lc", &script]).await?;
    let Some(first_line) = output
        .stdout
        .lines()
        .next()
        .filter(|l| !l.trim().is_empty())
    else {
        return Ok(None);
    };
    let banner: Value = serde_json::from_str(first_line).with_context(|| {
        format!(
            "a line came back on {dest}:{port} and it is not the identity JSON: \
             {first_line:?} (exit={} stderr={})",
            output.exit_code, output.stderr
        )
    })?;
    Ok(Some(banner))
}

async fn echo_round_trip(sandbox: &mut FirecrackerSandbox, dest: &str) -> Result<String> {
    let script = format!(
        "timeout 5 bash -c 'exec 3<>/dev/tcp/{dest}/443; IFS= read -r _banner <&3; \
         printf hello-from-guest >&3; exec 3>&-; ' >/dev/null 2>&1; \
         timeout 5 bash -c 'exec 3<>/dev/tcp/{dest}/443; IFS= read -r _banner <&3; \
         printf hello-from-guest >&3; head -c 16 <&3'"
    );
    let output = sandbox.run_command("bash", &["-lc", &script]).await?;
    Ok(output.stdout)
}

#[tokio::test]
async fn a_443_connection_reaches_the_broker_with_the_sandbox_identity_and_original_destination(
) -> Result<()> {
    common::setup().await;
    require_local_broker()?;
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy =
        Some(policy_with_rules(BaseSandboxNetworkPolicy::Default, None)?);

    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    let banner = brokered_banner(&mut sandbox, FAKE_UPSTREAM)
        .await?
        .context("the intercepted connection was refused")?;
    assert_eq!(banner["execution_id"], sandbox.execution_id().to_string());
    assert_eq!(banner["original_dst"], format!("{FAKE_UPSTREAM}:443"));
    assert_eq!(
        echo_round_trip(&mut sandbox, FAKE_UPSTREAM).await?,
        "hello-from-guest"
    );

    sandbox.stop().await?;
    Ok(())
}

#[tokio::test]
async fn the_broker_outlives_the_thread_that_asked_for_it() -> Result<()> {
    common::setup().await;
    let socket_path = ConfigManager::global_config()
        .egress_broker
        .socket_path
        .clone()
        .context("[egress_broker].socket_path is required in local mode")?;

    // The first ask from a thread that then ends, which is what every test in
    // this file is: a broker spawned on that thread takes SIGTERM with it and
    // leaves the socket gone for the next one.
    std::thread::spawn(require_local_broker)
        .join()
        .expect("the asking thread panicked")?;
    std::os::unix::net::UnixStream::connect(&socket_path)
        .with_context(|| format!("connect {socket_path:?} after the asking thread ended"))?;

    // The second ask gets the same broker, not a second one.
    require_local_broker()?;
    std::os::unix::net::UnixStream::connect(&socket_path)
        .with_context(|| format!("connect {socket_path:?} on the second ask"))?;

    Ok(())
}

#[tokio::test]
async fn removing_the_rules_stops_the_intercept_and_adding_them_back_restores_it() -> Result<()> {
    common::setup().await;
    require_local_broker()?;
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy =
        Some(policy_with_rules(BaseSandboxNetworkPolicy::Default, None)?);
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;
    assert!(brokered_banner(&mut sandbox, FAKE_UPSTREAM)
        .await?
        .is_some());

    sandbox.update_network_policy(None).await?;
    assert!(
        brokered_banner(&mut sandbox, FAKE_UPSTREAM)
            .await?
            .is_none(),
        "without rules the connection goes to the real destination, which does not answer"
    );

    sandbox
        .update_network_policy(Some(policy_with_rules(
            BaseSandboxNetworkPolicy::Default,
            None,
        )?))
        .await?;
    assert!(brokered_banner(&mut sandbox, FAKE_UPSTREAM)
        .await?
        .is_some());

    sandbox.stop().await?;
    Ok(())
}

#[tokio::test]
async fn rules_are_rebuilt_after_pause_and_resume_under_the_new_execution() -> Result<()> {
    common::setup().await;
    require_local_broker()?;
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy =
        Some(policy_with_rules(BaseSandboxNetworkPolicy::Default, None)?);
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;
    let before = brokered_banner(&mut sandbox, FAKE_UPSTREAM)
        .await?
        .context("brokered before pause")?;

    let snapshot = sandbox.pause().await?;
    sandbox.stop().await?;

    let mut resumed = FirecrackerSandbox::resume_from_snapshot_config(&snapshot).await?;
    let after = brokered_banner(&mut resumed, FAKE_UPSTREAM)
        .await?
        .context("brokered after resume")?;
    // The sandbox id is not asserted: this harness resumes without an api
    // half, and `resume_from_snapshot_config` mints a new one. What the
    // resume has to rebuild is the intercept and the identity behind it —
    // a new run of the same policy, under a new execution.
    assert_ne!(after["execution_id"], before["execution_id"]);
    assert_eq!(after["original_dst"], format!("{FAKE_UPSTREAM}:443"));
    assert_eq!(after["handler"], before["handler"]);
    resumed.stop().await?;
    Ok(())
}

#[tokio::test]
async fn a_sandbox_without_rules_gets_no_ca_and_no_intercept() -> Result<()> {
    common::setup().await;
    require_local_broker()?;
    let mut sandbox = FirecrackerSandbox::new(common::default_sandbox_config()?)?;
    sandbox.start().await?;
    assert!(brokered_banner(&mut sandbox, FAKE_UPSTREAM)
        .await?
        .is_none());
    let output = sandbox
        .run_command("sh", &["-c", "env | grep -c '^SSL_CERT_FILE=' || true"])
        .await?;
    assert_eq!(
        output.stdout.trim(),
        "0",
        "no trust env is injected without rules"
    );
    sandbox.stop().await?;
    Ok(())
}

#[tokio::test]
async fn a_reused_slot_carries_no_intercept_from_its_previous_tenant() -> Result<()> {
    common::setup().await;
    require_local_broker()?;
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy =
        Some(policy_with_rules(BaseSandboxNetworkPolicy::Default, None)?);
    let mut first = FirecrackerSandbox::new(sandbox_config)?;
    first.start().await?;
    assert!(brokered_banner(&mut first, FAKE_UPSTREAM).await?.is_some());
    first.stop().await?;

    let mut second = FirecrackerSandbox::new(common::default_sandbox_config()?)?;
    second.start().await?;
    assert!(
        brokered_banner(&mut second, FAKE_UPSTREAM).await?.is_none(),
        "the pooled namespace must not keep the previous tenant's DNAT"
    );
    second.stop().await?;
    Ok(())
}

/// One explicit endpoint on [`ENDPOINT_PORT`], validated the way the API
/// validates it and then pointed at the handler the embedded broker serves.
fn policy_with_endpoint(intercept_port: bool) -> Result<SandboxNetworkPolicy> {
    let mut policy = SandboxNetworkPolicy::new(
        BaseSandboxNetworkPolicy::Default,
        SandboxNetworkEgressPolicy::with_rules_and_endpoints(
            None,
            None,
            None,
            Some(vec![EndpointDeclaration {
                port: ENDPOINT_PORT,
                handler: "tcp".to_string(),
                params: serde_json::json!({"upstream": "fake.test:5432"}),
                intercept_port,
            }]),
        )?,
    );
    for broker in &mut policy.egress.brokers {
        broker.handler = "echo".to_string();
    }
    Ok(policy)
}

#[tokio::test]
async fn an_explicit_endpoint_answers_on_its_own_port_and_adds_no_trust_anchor() -> Result<()> {
    common::setup().await;
    require_local_broker()?;
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy = Some(policy_with_endpoint(false)?);
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    let banner = banner_on_port(&mut sandbox, BROKER_LISTENER_IP, ENDPOINT_PORT)
        .await?
        .context("the declared endpoint did not answer on its own port")?;
    assert_eq!(banner["execution_id"], sandbox.execution_id().to_string());
    assert_eq!(banner["port"], ENDPOINT_PORT);

    let output = sandbox
        .run_command("sh", &["-c", "env | grep -c '^SSL_CERT_FILE=' || true"])
        .await?;
    assert_eq!(
        output.stdout.trim(),
        "0",
        "an endpoint the guest reaches in the clear needs no CA in the guest"
    );
    assert!(
        banner_on_port(&mut sandbox, FAKE_UPSTREAM, ENDPOINT_PORT)
            .await?
            .is_none(),
        "without intercept_port the guest reaches the real destination, which does not answer"
    );

    sandbox.stop().await?;
    Ok(())
}

#[tokio::test]
async fn intercept_port_captures_any_host_and_stops_when_it_is_turned_off() -> Result<()> {
    common::setup().await;
    require_local_broker()?;
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy = Some(policy_with_endpoint(true)?);
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    let banner = banner_on_port(&mut sandbox, FAKE_UPSTREAM, ENDPOINT_PORT)
        .await?
        .context("intercept_port did not capture a connection to another host")?;
    assert_eq!(
        banner["original_dst"],
        format!("{FAKE_UPSTREAM}:{ENDPOINT_PORT}")
    );

    sandbox
        .update_network_policy(Some(policy_with_endpoint(false)?))
        .await?;
    assert!(
        banner_on_port(&mut sandbox, FAKE_UPSTREAM, ENDPOINT_PORT)
            .await?
            .is_none(),
        "turning intercept_port off must release the port"
    );
    assert!(
        banner_on_port(&mut sandbox, BROKER_LISTENER_IP, ENDPOINT_PORT)
            .await?
            .is_some(),
        "the listener itself stays reachable at its own address"
    );

    sandbox.stop().await?;
    Ok(())
}

#[tokio::test]
async fn rules_and_an_intercepting_endpoint_each_keep_their_own_redirect() -> Result<()> {
    common::setup().await;
    require_local_broker()?;
    let mut policy = policy_with_rules(BaseSandboxNetworkPolicy::Default, None)?;
    policy
        .egress
        .brokers
        .extend(policy_with_endpoint(true)?.egress.brokers.into_iter());
    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.common.network_policy = Some(policy);
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;

    // One namespace holds one intercept chain, so a per-endpoint install would
    // leave only the last DNAT standing.
    assert!(
        brokered_banner(&mut sandbox, FAKE_UPSTREAM)
            .await?
            .is_some(),
        "the rules intercept on 443 must survive the endpoint's own"
    );
    assert!(
        banner_on_port(&mut sandbox, FAKE_UPSTREAM, ENDPOINT_PORT)
            .await?
            .is_some(),
        "the endpoint intercept must survive the rules intercept"
    );

    sandbox.stop().await?;
    Ok(())
}
