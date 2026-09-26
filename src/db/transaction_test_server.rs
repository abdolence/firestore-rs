use crate::{FirestoreDb, FirestoreDbOptions};
use gcloud_sdk::tonic::Code;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};

pub(super) struct TestServer {
    pub db: FirestoreDb,
    task: JoinHandle<()>,
}

impl TestServer {
    // The handler receives an unframed protobuf message. None holds the response open.
    pub async fn start<F>(handler: F) -> Self
    where
        F: Fn(&str, &[u8]) -> Option<(Code, Vec<u8>)> + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let handler = handler.clone();
                        connections.spawn(async move {
                            let mut connection = h2::server::handshake(socket).await.unwrap();
                            let mut requests = JoinSet::new();
                            loop {
                                tokio::select! {
                                    incoming = connection.accept() => {
                                        let Some(Ok((request, mut respond))) = incoming else { break; };
                                        let handler = handler.clone();
                                        requests.spawn(async move {
                                            let method = request.uri().path().to_owned();
                                            let mut body = request.into_body();
                                            let mut bytes = Vec::new();
                                            while let Some(chunk) = body.data().await {
                                                let chunk = chunk.unwrap();
                                                body.flow_control().release_capacity(chunk.len()).unwrap();
                                                bytes.extend_from_slice(&chunk);
                                            }
                                            assert!(bytes.len() >= 5 && bytes[0] == 0);
                                            let Some((code, message)) = handler(&method, &bytes[5..]) else {
                                                return futures::future::pending().await;
                                            };
                                            let headers = hyper::Response::builder()
                                                .status(200)
                                                .header("content-type", "application/grpc");
                                            if code != Code::Ok {
                                                let _ = respond.send_response(headers
                                                    .header("grpc-status", (code as i32).to_string())
                                                    .body(()).unwrap(), true);
                                            } else {
                                                let Ok(mut output) = respond.send_response(headers.body(()).unwrap(), false) else { return; };
                                                let mut frame = vec![0];
                                                frame.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
                                                frame.extend_from_slice(&message);
                                                if output.send_data(frame.into(), false).is_err() { return; }
                                                let mut trailers = hyper::HeaderMap::new();
                                                trailers.insert("grpc-status", hyper::header::HeaderValue::from_static("0"));
                                                let _ = output.send_trailers(trailers);
                                            }
                                        });
                                    }
                                    Some(result) = requests.join_next() => result.unwrap(),
                                }
                            }
                        });
                    }
                    Some(result) = connections.join_next() => result.unwrap(),
                }
            }
        });
        let mut options = FirestoreDbOptions::new("transaction-test".into());
        options.firebase_api_url = Some(endpoint);
        let db = FirestoreDb::with_options_token_source(
            options,
            Vec::new(),
            gcloud_sdk::TokenSourceType::ExternalSource(Box::new(
                crate::db::FirestoreEmulatorTokenSource,
            )),
        )
        .await
        .unwrap();
        Self { db, task }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
