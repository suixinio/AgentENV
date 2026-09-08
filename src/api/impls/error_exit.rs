//! The last hop of an error: from the status-carrying `models::Error` every
//! mapping in this layer already produces, to the variant of one endpoint's
//! generated response enum.

use agentenv_http_server::models;

/// One endpoint's error exit.
///
/// Every generated response enum spells the same statuses under its own
/// variant names, which is why a shared mapping stops one step short of the
/// response. This trait is that step, and [`api_error_exit`] writes it from
/// the variants an endpoint actually declares.
pub(in crate::api) trait ApiErrorExit: Sized {
    /// The response carrying `error`, chosen by its status code.
    fn exit(error: models::Error) -> Self;
}

/// Implements [`ApiErrorExit`] for one response enum.
///
/// The fallback arm is what a code the endpoint does not declare becomes: an
/// endpoint that grows a status without listing it here keeps answering, and
/// answers it as a server error rather than failing to compile.
macro_rules! api_error_exit {
    ($resp:ty { $($code:literal => $variant:ident),+ $(,)? } _ => $fallback:ident) => {
        impl $crate::api::impls::error_exit::ApiErrorExit for $resp {
            fn exit(error: agentenv_http_server::models::Error) -> Self {
                match error.code {
                    $($code => Self::$variant(error),)+
                    _ => Self::$fallback(error),
                }
            }
        }
    };
}

pub(in crate::api) use api_error_exit;

#[cfg(test)]
mod tests {
    use super::*;
    use agentenv_http_server::apis::sandboxes::*;

    #[test]
    fn a_status_the_endpoint_declares_takes_its_own_variant() {
        assert!(matches!(
            SandboxesSandboxIdConnectPostResponse::exit(models::Error::new(404, "gone".into())),
            SandboxesSandboxIdConnectPostResponse::Status404_NotFound(_)
        ));
        assert!(matches!(
            SandboxesSandboxIdResumePostResponse::exit(models::Error::new(409, "busy".into())),
            SandboxesSandboxIdResumePostResponse::Status409_Conflict(_)
        ));
    }

    #[test]
    fn a_status_the_endpoint_does_not_declare_becomes_its_server_error() {
        assert!(matches!(
            SandboxesSandboxIdResumePostResponse::exit(models::Error::new(400, "bad".into())),
            SandboxesSandboxIdResumePostResponse::Status500_ServerError(_)
        ));
    }
}
