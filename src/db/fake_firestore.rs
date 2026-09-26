use crate::db::FirestoreEmulatorTokenSource;
use crate::{FirestoreDb, FirestoreDbOptions};
use gcloud_sdk::google::firestore::v1::{
    get_document_request, BeginTransactionResponse, CommitResponse, GetDocumentRequest,
};
use gcloud_sdk::prost::Message as _;
use gcloud_sdk::tonic::Code;
use h2::server::SendResponse;
use h2::RecvStream;
use hyper::body::Bytes;
use hyper::header::HeaderValue;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Notify};

/// A handler's answer to one RPC.
pub(super) enum FakeResponse {
    /// A framed protobuf message, sent with a `grpc-status: 0` trailer.
    Message(Vec<u8>),
    /// A Trailers-Only error response with no body - the only way a streaming call's error is
    /// visible at its initial `await` rather than only once polled. Never `Code::Ok`: a unary
    /// call answered that way fails for want of a response message; use [`FakeResponse::empty`].
    Status(Code),
    /// The server closes the whole connection without answering, simulating a response lost in
    /// transit: the client sees a transport error, not a `grpc-status` it can read off the wire.
    Drop,
}

impl FakeResponse {
    /// A successful answer carrying an empty message, such as `Rollback`'s `Empty`.
    pub fn empty() -> Self {
        Self::Message(Vec::new())
    }

    /// A successful `Commit` with no write results.
    pub fn committed() -> Self {
        Self::Message(CommitResponse::default().encode_to_vec())
    }
}

/// Answers `BeginTransaction` with the next sequential transaction ID (1, 2, 3, ...), logged as
/// `Begin→<id>` - the one RPC every transaction fixture in this crate answers the same way.
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

/// Answers `BeginTransaction` with `code` instead, logged as `Begin failed: <code>`. No
/// transaction ID is used up, so the next [`begin_response`] opens the one this would have.
pub(super) fn begin_failure(code: Code) -> (String, FakeResponse) {
    (
        format!("Begin failed: {code:?}"),
        FakeResponse::Status(code),
    )
}

/// The transaction ID, as [`begin_response`] numbers them, that a `GetDocument` request reads in.
///
/// Panics if the read does not belong to a transaction.
pub(super) fn read_transaction_id(get_document_request: &[u8]) -> u8 {
    let request = GetDocumentRequest::decode(get_document_request).unwrap();
    let Some(get_document_request::ConsistencySelector::Transaction(id)) =
        request.consistency_selector
    else {
        panic!("read must belong to a transaction");
    };
    id[0]
}

type Handler = dyn Fn(&str, &[u8]) -> (String, FakeResponse) + Send + Sync;

/// A minimal fake Firestore gRPC server for driving a real `FirestoreDb` against a controlled
/// backend. `handler` runs once per RPC, after its request body has ended, and returns one line
/// for `calls()` plus the response - so a test can assert a whole RPC sequence in one comparison.
///
/// `Listen` is the exception: it is answered at once with an empty stream, and its handler runs
/// only when the client closes the request, so a logged `Listen` call is itself the proof that
/// the request was closed. The handler's response to it is ignored.
pub(super) struct FakeFirestore {
    pub db: FirestoreDb,
    calls: Arc<watch::Sender<Vec<String>>>,
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
        let handler: Arc<Handler> = Arc::new(handler);
        let calls = Arc::new(watch::Sender::new(Vec::new()));
        let accepted = calls.clone();
        // Detached: #[tokio::test] drops every spawned task, this one included, at test end.
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve_connection(socket, handler.clone(), accepted.clone()));
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
        self.calls.borrow().clone()
    }

    /// Resolves once at least `count` RPCs have been handled.
    pub async fn wait_for_calls(&self, count: usize) {
        self.calls
            .subscribe()
            .wait_for(|calls| calls.len() >= count)
            .await
            .expect("the call log lives as long as the server that owns it");
    }
}

/// Serves one client connection until the client closes it or an RPC answers
/// [`FakeResponse::Drop`], which drops the connection and its socket.
async fn serve_connection(
    socket: TcpStream,
    handler: Arc<Handler>,
    calls: Arc<watch::Sender<Vec<String>>>,
) {
    let Ok(mut connection) = h2::server::handshake(socket).await else {
        return;
    };
    let close = Arc::new(Notify::new());
    let serve = async {
        while let Some(Ok((request, respond))) = connection.accept().await {
            tokio::spawn(answer(
                request,
                respond,
                handler.clone(),
                calls.clone(),
                close.clone(),
            ));
        }
    };
    tokio::select! {
        () = serve => {}
        () = close.notified() => {}
    }
}

async fn answer(
    request: hyper::Request<RecvStream>,
    mut respond: SendResponse<Bytes>,
    handler: Arc<Handler>,
    calls: Arc<watch::Sender<Vec<String>>>,
    close: Arc<Notify>,
) {
    let method = request.uri().path().to_owned();
    let listening = method.ends_with("/Listen");
    if listening {
        let headers = ok_headers().header("grpc-status", "0");
        if respond
            .send_response(headers.body(()).unwrap(), true)
            .is_err()
        {
            return;
        }
    }
    let mut body = request.into_body();
    let mut bytes = Vec::new();
    while let Some(Ok(chunk)) = body.data().await {
        body.flow_control().release_capacity(chunk.len()).unwrap();
        bytes.extend_from_slice(&chunk);
    }
    let (call, response) = handler(&method, bytes.get(5..).unwrap_or_default());
    calls.send_modify(|calls| calls.push(call));
    if listening {
        return;
    }
    match response {
        FakeResponse::Message(message) => {
            let Ok(mut send) = respond.send_response(ok_headers().body(()).unwrap(), false) else {
                return;
            };
            let mut frame = vec![0u8; 5];
            frame[1..5].copy_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
            frame.extend_from_slice(&message);
            if send.send_data(frame.into(), false).is_err() {
                return;
            }
            let mut trailers = hyper::HeaderMap::new();
            trailers.insert("grpc-status", HeaderValue::from_static("0"));
            let _ = send.send_trailers(trailers);
        }
        // A single Trailers-Only frame, with no body ever opened, is what makes even a streaming
        // call's error visible at its initial `await` rather than only once its caller polls the
        // response.
        FakeResponse::Status(code) => {
            assert_ne!(code, Code::Ok, "answer success with FakeResponse::empty");
            let headers = ok_headers().header("grpc-status", (code as i32).to_string());
            let _ = respond.send_response(headers.body(()).unwrap(), true);
        }
        FakeResponse::Drop => {
            close.notify_one();
            // Dropping `respond` while the connection is still up would reset just this stream,
            // which the client reads as CANCELLED rather than as a lost connection. The task ends
            // with the test's runtime.
            std::future::pending::<()>().await;
        }
    }
}

fn ok_headers() -> hyper::http::response::Builder {
    hyper::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
}
