use crate::db::FirestoreEmulatorTokenSource;
use crate::{FirestoreDb, FirestoreDbOptions};
use gcloud_sdk::google::firestore::v1::BeginTransactionResponse;
use gcloud_sdk::prost::Message as _;
use gcloud_sdk::tonic::Code;
use hyper::header::HeaderValue;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

/// A handler's answer to one RPC.
pub(super) enum FakeResponse {
    /// A framed protobuf message, sent with a `grpc-status: 0` trailer.
    Message(Vec<u8>),
    /// A Trailers-Only response with no body - the only way a streaming call's error is visible
    /// at its initial `await` rather than only once polled. `Code::Ok` here is not an error.
    Status(Code),
    /// The connection drops with no response, simulating one lost in transit - a transport
    /// error on the client, not a `grpc-status` it can read off the wire.
    Drop,
}

/// The next sequential transaction id (1, 2, 3, ...) and a framed `BeginTransaction` response
/// naming it - the one RPC every transaction fixture in this crate answers the same way.
pub(super) fn begin_response(next_id: &AtomicU8) -> (String, FakeResponse) {
    let id = next_id.fetch_add(1, Ordering::SeqCst) + 1;
    let transaction = BeginTransactionResponse {
        transaction: vec![id],
    };
    (
        format!("Begin→{id}"),
        FakeResponse::Message(transaction.encode_to_vec()),
    )
}

fn ok_headers() -> hyper::http::response::Builder {
    hyper::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
}

/// A minimal fake Firestore gRPC server for driving a real `FirestoreDb` against a controlled
/// backend. `handler` runs once per RPC, after its request body has ended, and returns one line
/// for `calls()` plus the response - so a test can assert a whole RPC sequence in one comparison.
pub(super) struct FakeFirestore {
    pub db: FirestoreDb,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeFirestore {
    pub async fn start<F>(handler: F) -> Self
    where
        F: Fn(&str, &[u8]) -> (String, FakeResponse) + Send + Sync + 'static,
    {
        Self::start_with_max_retries(3, handler).await
    }

    /// Like [`start`](Self::start), but with a `max_retries` other than the client default -
    /// enough for a test to bound a persistent failure to a small, fast number of attempts.
    pub async fn start_with_max_retries<F>(max_retries: usize, handler: F) -> Self
    where
        F: Fn(&str, &[u8]) -> (String, FakeResponse) + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handler = Arc::new(handler);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let accepted = calls.clone();
        // Detached: #[tokio::test] drops every spawned task, this one included, at test end.
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let mut connection = h2::server::handshake(socket).await.unwrap();
                let (handler, calls) = (handler.clone(), accepted.clone());
                tokio::spawn(async move {
                    while let Some(Ok((request, mut respond))) = connection.accept().await {
                        let (handler, calls) = (handler.clone(), calls.clone());
                        tokio::spawn(async move {
                            let method = request.uri().path().to_owned();
                            let listening = method.ends_with("/Listen");
                            let mut open =
                                || respond.send_response(ok_headers().body(()).unwrap(), false);
                            if listening && open().is_err() {
                                return;
                            }
                            let mut body = request.into_body();
                            let mut bytes = Vec::new();
                            while let Some(Ok(chunk)) = body.data().await {
                                body.flow_control().release_capacity(chunk.len()).unwrap();
                                bytes.extend_from_slice(&chunk);
                            }
                            let (call, response) =
                                handler(&method, bytes.get(5..).unwrap_or_default());
                            calls.lock().unwrap().push(call);
                            if listening {
                                // The client dropped the response before this ran - the only way
                                // Listen's otherwise endless body could have ended - so there is
                                // nothing left to answer.
                                return;
                            }
                            match response {
                                FakeResponse::Message(message) => {
                                    let Ok(mut send) = open() else { return };
                                    let mut frame = vec![0u8; 5];
                                    frame[1..5].copy_from_slice(
                                        &u32::try_from(message.len()).unwrap().to_be_bytes(),
                                    );
                                    frame.extend_from_slice(&message);
                                    if send.send_data(frame.into(), false).is_err() {
                                        return;
                                    }
                                    let mut trailers = hyper::HeaderMap::new();
                                    trailers.insert("grpc-status", HeaderValue::from_static("0"));
                                    let _ = send.send_trailers(trailers);
                                }
                                // A single Trailers-Only frame, with no body ever opened, is what
                                // makes even a streaming call's error visible at its initial
                                // `await` rather than only once its caller polls the response.
                                FakeResponse::Status(code) => {
                                    let headers = ok_headers()
                                        .header("grpc-status", (code as i32).to_string());
                                    let _ = respond.send_response(headers.body(()).unwrap(), true);
                                }
                                FakeResponse::Drop => {}
                            }
                        });
                    }
                });
            }
        });
        let mut options = FirestoreDbOptions::new("fake-firestore".into());
        options.firebase_api_url = Some(endpoint);
        options.max_retries = max_retries;
        let db = FirestoreDb::with_options_token_source(
            options,
            Vec::new(),
            gcloud_sdk::TokenSourceType::ExternalSource(Box::new(FirestoreEmulatorTokenSource)),
        )
        .await
        .unwrap();
        Self { db, calls }
    }

    /// Every RPC handled so far, in the order the server received it.
    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}
