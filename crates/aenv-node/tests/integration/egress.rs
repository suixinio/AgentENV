//! Brokered egress with the embedded broker: the guest's 443 traffic is
//! intercepted in its namespace, reaches the broker with the right identity
//! and original destination, and the listeners follow the policy through
//! updates, pause/resume and slot reuse.
//!
//! The embedded broker dispatches the `tcp` identity-echo handler only, so
//! these policies name that handler where a public `rules` policy names
//! `http`; everything between the guest and the broker is the production
//! path. Requires root, `/dev/kvm`, and a node config with
//! `[egress_broker].mode = "embedded"` and `[cluster].node_discovery_mode =
//! "static"`. Passthrough of unmatched SNI and policy denial at the broker
//! need the `http` handler and are covered when it lands.

use aenv_node::cfg::{ConfigManager, EgressBrokerMode};
use aenv_node::sandbox::network::policy::{DomainRule, HeaderTransform};
use aenv_node::sandbox::{
    BaseSandboxNetworkPolicy, FirecrackerSandbox, SandboxBackend, SandboxExecutor,
    SandboxNetworkEgressPolicy, SandboxNetworkPolicy,
};
use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::common;

const FAKE_UPSTREAM: &str = "203.0.113.10";

fn require_embedded_broker() -> Result<()> {
    let mode = ConfigManager::global_config().egress_broker.mode;
    if mode != EgressBrokerMode::Embedded {
        bail!(
            "these tests need [egress_broker].mode = \"embedded\" in the node config; it is {}",
            mode.as_str()
        );
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
    // `with_rules` names the `http` handler, which lives in the broker
    // process; the embedded dispatcher would answer `unknown_handler`.
    for broker in &mut policy.egress.brokers {
        broker.handler = "tcp".to_string();
    }
    Ok(policy)
}

/// Connects to `dest:443` from the guest, reads the identity banner the
/// `tcp` handler answers with, then proves the echo path.
async fn brokered_banner(sandbox: &mut FirecrackerSandbox, dest: &str) -> Result<Option<Value>> {
    let script = format!(
        "timeout 5 bash -c 'exec 3<>/dev/tcp/{dest}/443 || exit 7; \
         IFS= read -r line <&3; printf \"%s\\n\" \"$line\"; \
         printf ping >&3; exec 3>&-; ' 2>&1"
    );
    let output = sandbox.run_command("bash", &["-lc", &script]).await?;
    if output.exit_code == 7 {
        return Ok(None);
    }
    let first_line = output.stdout.lines().next().with_context(|| {
        format!(
            "no banner from the broker; exit={} stderr={}",
            output.exit_code, output.stderr
        )
    })?;
    let banner: Value = serde_json::from_str(first_line)
        .with_context(|| format!("banner is not the identity JSON: {first_line:?}"))?;
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
    require_embedded_broker()?;
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
async fn removing_the_rules_stops_the_intercept_and_adding_them_back_restores_it() -> Result<()> {
    common::setup().await;
    require_embedded_broker()?;
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
    require_embedded_broker()?;
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
    assert_eq!(after["sandbox_id"], before["sandbox_id"]);
    assert_ne!(after["execution_id"], before["execution_id"]);
    assert_eq!(after["original_dst"], format!("{FAKE_UPSTREAM}:443"));
    resumed.stop().await?;
    Ok(())
}

#[tokio::test]
async fn a_sandbox_without_rules_gets_no_ca_and_no_intercept() -> Result<()> {
    common::setup().await;
    require_embedded_broker()?;
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
    require_embedded_broker()?;
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
