use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper::Uri;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tower::Service;

#[derive(Clone)]
pub struct UnixConnector {
    pub path: PathBuf,
}

impl Service<Uri> for UnixConnector {
    type Response = TokioIo<UnixStream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Uri) -> Self::Future {
        let path = self.path.clone();
        Box::pin(async move {
            let stream = UnixStream::connect(path).await?;
            Ok(TokioIo::new(stream))
        })
    }
}

impl Connection for UnixConnector {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker;
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    #[test]
    fn poll_ready_is_immediately_ready() {
        let mut connector = UnixConnector {
            path: PathBuf::from("/tmp/nonexistent.sock"),
        };
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(connector.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    }

    #[test]
    fn connected_is_available() {
        let connector = UnixConnector {
            path: PathBuf::from("/tmp/nonexistent.sock"),
        };
        let _ = connector.connected();
    }

    #[tokio::test]
    async fn call_returns_error_for_missing_socket() {
        let temp = tempdir().expect("tempdir");
        let socket_path = temp.path().join("missing.sock");
        let mut connector = UnixConnector { path: socket_path };

        let err = connector
            .call(Uri::from_static("http://localhost"))
            .await
            .expect_err("missing socket should fail");

        assert!(
            matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ),
            "unexpected error kind: {err:?}"
        );
    }

    #[tokio::test]
    async fn call_connects_to_bound_unix_listener() {
        let temp = tempdir().expect("tempdir");
        let socket_path = temp.path().join("firecracker.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind unix socket");
        let accept_task = tokio::spawn(async move {
            let _ = listener.accept().await.expect("accept connection");
        });

        let mut connector = UnixConnector { path: socket_path };
        let stream = connector
            .call(Uri::from_static("http://localhost"))
            .await
            .expect("connect to listener");
        drop(stream);
        accept_task.await.expect("listener task");
    }
}
