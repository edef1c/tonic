use crate::body::Body;
use http::{Request, Response};
use shared_http_body::SharedBodyExt;
use std::{
    error::Error as StdError,
    future,
    pin::Pin,
    task::{Context, Poll},
};
use tower_service::Service;

type BoxFuture =
    Pin<Box<dyn future::Future<Output = Result<Response<Body>, crate::BoxError>> + Send>>;

/// Returns true if the error indicates a request that was never processed by
/// the server and is unconditionally safe to retry per RFC 9113 §6.8 and gRFC A6.
///
/// This covers four cases:
/// - GOAWAY with NO_ERROR: the server is shutting down gracefully, streams that
///   tried to open after conn_error was set get this error
/// - REFUSED_STREAM: streams that had HEADERS on the wire but whose IDs were
///   above last_stream_id get RST_STREAM with REFUSED_STREAM
/// - Canceled: requests queued in hyper's dispatch channel that never reached
///   h2::SendRequest::send_request() — the channel was dropped when the
///   connection task exited on Dispatched::Shutdown
/// - DispatchGone: the h2 connection task exited (e.g. after draining a GOAWAY)
///   while requests were still queued in hyper's dispatch channel — the
///   Callback::drop() impl sends this error to waiting callers
fn is_retryable_stream_rejection(err: &crate::BoxError) -> bool {
    if let Some(hyper_err) = err.downcast_ref::<hyper::Error>() {
        // GOAWAY with NO_ERROR
        if hyper_err.h2_go_away_reason() == Some(0) {
            return true;
        }

        // Canceled: request never left hyper's dispatch channel
        if hyper_err.is_canceled() {
            return true;
        }

        // DispatchGone: connection task exited while requests were queued.
        // This is the typical error path when GOAWAY causes the h2 connection
        // to drain and exit — pending Callback objects are dropped, producing
        // this error for any request still in the dispatch channel.
        if hyper_err.is_dispatch_gone() {
            return true;
        }

        // REFUSED_STREAM via h2::Error in the source chain
        let mut current = StdError::source(hyper_err);
        while let Some(e) = current {
            if let Some(h2_err) = e.downcast_ref::<h2::Error>() {
                return h2_err.is_remote() && h2_err.reason() == Some(h2::Reason::REFUSED_STREAM);
            }
            current = e.source();
        }
    }

    false
}

/// Service layer that transparently retries requests rejected before processing.
///
/// When an HTTP/2 server sends GOAWAY with NO_ERROR or resets a stream with
/// REFUSED_STREAM, the request was never processed by the server. Per gRFC A6,
/// these are unconditionally safe to retry. This layer detects such failures
/// and retries on a fresh connection.
///
/// Request bodies are made cloneable via [`SharedBody`] so they can be replayed.
/// The clone is dropped as soon as the first attempt succeeds (Response-Headers
/// received = RPC committed per gRFC A6), freeing the buffer.
pub(crate) struct GoawayRetry<S> {
    inner: S,
}

impl<S: Clone> Clone for GoawayRetry<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S> GoawayRetry<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<Request<Body>> for GoawayRetry<S>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = crate::BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = crate::BoxError;
    type Future = BoxFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        // Make the body cloneable for potential retry.
        let (parts, body) = req.into_parts();
        let shared = body.into_shared();
        let retry_body = shared.clone();
        let retry_parts = parts.clone();

        let first_req = Request::from_parts(parts, Body::new(shared));
        let first_fut = self.inner.call(first_req);

        // Clone the service handle for retry (Buffer clone is cheap).
        let mut retry_svc = self.inner.clone();

        Box::pin(async move {
            let result = first_fut.await;

            // If the request was rejected before processing (GOAWAY or
            // REFUSED_STREAM), retry on a fresh connection. We may need
            // multiple attempts because the underlying Reconnect layer
            // might still dispatch to the dying connection on the first
            // retry (the h2 connection stays alive to drain accepted
            // streams, so hyper's is_closed() returns false briefly).
            let mut last_err = match result {
                Ok(res) => return Ok(res),
                Err(e) if is_retryable_stream_rejection(&e) => e,
                Err(e) => return Err(e),
            };

            for _ in 0..3 {
                tracing::debug!("retrying request rejected before processing");
                future::poll_fn(|cx| retry_svc.poll_ready(cx)).await?;
                let req = Request::from_parts(retry_parts.clone(), Body::new(retry_body.clone()));
                match retry_svc.call(req).await {
                    Ok(res) => return Ok(res),
                    Err(e) if is_retryable_stream_rejection(&e) => last_err = e,
                    Err(e) => return Err(e),
                }
            }
            Err(last_err)
        })
    }
}
