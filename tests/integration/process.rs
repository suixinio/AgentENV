use crate::common;

use std::collections::HashMap;

use aenv_core::sandbox::{FirecrackerSandbox, ProcessOpts, SandboxExecutor, Signal};

use anyhow::Result;
use tokio::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Helper: create and start a sandbox for process tests.
async fn start_sandbox() -> Result<FirecrackerSandbox> {
    let sandbox_config = common::default_sandbox_config()?;
    let mut sandbox = FirecrackerSandbox::new(sandbox_config)?;
    sandbox.start().await?;
    Ok(sandbox)
}

#[tokio::test]
async fn run_command_basic_contracts() -> Result<()> {
    common::setup().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut sandbox = start_sandbox().await?;

        // Verifies running a simple command and capturing stdout.
        let output = sandbox.run_command("echo", &["hello", "world"]).await?;
        assert_eq!(output.exit_code, 0);
        assert!(
            output.stdout.contains("hello world"),
            "expected 'hello world' in stdout, got: {:?}",
            output.stdout
        );
        assert!(output.stderr.is_empty(), "stderr should be empty");

        // Verifies that stderr is captured separately from stdout.
        let output = sandbox
            .run_command("sh", &["-c", "echo errout >&2"])
            .await?;
        assert_eq!(output.exit_code, 0);
        assert!(
            output.stderr.contains("errout"),
            "expected 'errout' in stderr, got: {:?}",
            output.stderr
        );

        // Verifies that a non-zero exit code is reported correctly.
        let output = sandbox.run_command("sh", &["-c", "exit 42"]).await?;
        assert_eq!(output.exit_code, 42);

        // Verifies that environment variables and cwd options are honoured.
        let mut envs = HashMap::new();
        envs.insert("MY_VAR".to_string(), "my_value".to_string());
        let opts = ProcessOpts::new().with_envs(envs).with_cwd("/tmp");
        let output = sandbox
            .run_command_with_opts("sh", &["-c", "echo $MY_VAR && pwd"], &opts)
            .await?;
        assert_eq!(output.exit_code, 0);
        assert!(
            output.stdout.contains("my_value"),
            "expected env var in stdout, got: {:?}",
            output.stdout
        );
        assert!(
            output.stdout.contains("/tmp"),
            "expected /tmp in cwd output, got: {:?}",
            output.stdout
        );

        sandbox.stop().await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}

#[tokio::test]
async fn run_command_handles_timeout() -> Result<()> {
    common::setup().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut sandbox = start_sandbox().await?;

        let opts = ProcessOpts::new().with_timeout(Duration::from_secs(1));

        let output = sandbox.run_command_with_opts("sleep", &["5"], &opts).await;

        assert!(
            output.is_err(),
            "expected timeout error, but command completed successfully"
        );

        sandbox.stop().await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}

#[tokio::test]
async fn run_command_with_unbounded_output() -> Result<()> {
    common::setup().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut sandbox = start_sandbox().await?;

        // Produce a deterministic amount of output that exceeds MAX_OUTPUT_BYTES
        // without relying on an infinite stream to hit the threshold in time.
        let output = sandbox
            .run_command("sh", &["-c", "yes line_of_output | head -c 11534336"])
            .await;

        assert!(
            output.is_err(),
            "expected error due to excessive output, but command completed successfully"
        );

        sandbox.stop().await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}

#[tokio::test]
async fn process_handle_supports_stdin_and_signal() -> Result<()> {
    common::setup().await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut sandbox = start_sandbox().await?;

        let mut handle = sandbox
            .start_process(
                "sh",
                &["-c", "IFS= read -r line && printf '%s\\n' \"$line\""],
                &ProcessOpts::default(),
            )
            .await?;

        assert!(handle.pid() > 0, "pid should be non-zero");

        // Send one line of stdin; the shell exits after echoing it back.
        handle.send_stdin(b"interactive_test\n").await?;

        let output = handle.wait().await?;
        assert_eq!(output.exit_code, 0);
        assert!(
            output.stdout.contains("interactive_test"),
            "expected echoed input in stdout, got: {:?}",
            output.stdout
        );

        let mut handle = sandbox
            .start_process("sleep", &["300"], &ProcessOpts::default())
            .await?;
        assert!(handle.pid() > 0);

        handle.send_signal(Signal::Sigterm).await?;
        let output = handle.wait().await?;
        assert_ne!(output.exit_code, 0);

        sandbox.stop().await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("test timed out"))?
}
