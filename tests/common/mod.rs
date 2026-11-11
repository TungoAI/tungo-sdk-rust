//! A small HTTP/1.1 mock server for the test suite. It speaks plain HTTP on
//! 127.0.0.1, records every request and lets a test script the answers byte
//! by byte (delays, dropped connections, endless bodies).

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tungo_sdk::{Tungo, TungoBuilder};

/// One request the mock received.
#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    /// The request target: path and query.
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// The index of the TCP connection that carried the request.
    pub connection: usize,
}

impl Recorded {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or("")
    }

    pub fn query(&self) -> &str {
        self.target.split_once('?').map_or("", |(_, query)| query)
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("the request body is JSON")
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// What the mock answers.
#[derive(Debug, Clone)]
pub enum Reply {
    /// A complete response with a Content-Length.
    Full {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    /// A response without Content-Length: each chunk is written after its
    /// delay in milliseconds, then the connection is closed.
    Stream {
        status: u16,
        headers: Vec<(String, String)>,
        chunks: Vec<(u64, Vec<u8>)>,
    },
    /// Close the connection without answering.
    Drop,
    /// Read the request and never answer.
    Hang,
    /// Send the headers and the first part of the body, then stall.
    StallBody { status: u16, body: Vec<u8> },
}

impl Reply {
    pub fn json(status: u16, body: &str) -> Reply {
        Reply::Full {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: body.as_bytes().to_vec(),
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Reply {
        match &mut self {
            Reply::Full { headers, .. } | Reply::Stream { headers, .. } => {
                headers.push((name.to_owned(), value.to_owned()));
            }
            _ => panic!("this reply has no headers"),
        }
        self
    }

    /// An event stream whose chunks are written `delay_ms` apart.
    pub fn sse(delay_ms: u64, chunks: &[&str]) -> Reply {
        Reply::Stream {
            status: 200,
            headers: vec![("content-type".into(), "text/event-stream".into())],
            chunks: chunks
                .iter()
                .map(|chunk| (delay_ms, chunk.as_bytes().to_vec()))
                .collect(),
        }
    }

    /// The API error envelope.
    pub fn error(status: u16, kind: &str, code: &str, message: &str) -> Reply {
        let body = serde_json::json!({
            "type": "error",
            "error": {"type": kind, "code": code, "message": message},
            "request_id": "req_test_0001",
        });
        Reply::json(status, &body.to_string())
    }
}

type Handler = dyn Fn(&Recorded, usize) -> Reply + Send + Sync;

struct State {
    handler: Box<Handler>,
    requests: Mutex<Vec<Recorded>>,
    connections: AtomicUsize,
    aborted_streams: AtomicUsize,
    finished_streams: AtomicUsize,
}

/// A running mock server. It stops when dropped.
pub struct Mock {
    pub url: String,
    state: Arc<State>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Mock {
    /// Starts a mock. `handler` gets each request and its 0-based index.
    pub async fn start<F>(handler: F) -> Mock
    where
        F: Fn(&Recorded, usize) -> Reply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let state = Arc::new(State {
            handler: Box::new(handler),
            requests: Mutex::new(Vec::new()),
            connections: AtomicUsize::new(0),
            aborted_streams: AtomicUsize::new(0),
            finished_streams: AtomicUsize::new(0),
        });
        let accept_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let index = accept_state.connections.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve(socket, index, Arc::clone(&accept_state)));
            }
        });
        Mock { url, state, task }
    }

    /// A mock that answers every request with the same JSON body.
    pub async fn always(status: u16, body: &'static str) -> Mock {
        Mock::start(move |_, _| Reply::json(status, body)).await
    }

    /// A client builder pointed at the mock, isolated from the environment.
    pub fn builder(&self) -> TungoBuilder {
        Tungo::builder()
            .ignore_env()
            .base_url(&self.url)
            .api_key("tg_test_SECRETKEY123")
            .company_id("co_1")
            .workspace_id("ws_1")
    }

    pub fn client(&self) -> Tungo {
        self.builder().build().expect("client")
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.state.requests.lock().expect("lock").clone()
    }

    pub fn count(&self) -> usize {
        self.state.requests.lock().expect("lock").len()
    }

    pub fn last(&self) -> Recorded {
        self.requests().pop().expect("the mock received a request")
    }

    pub fn connections(&self) -> usize {
        self.state.connections.load(Ordering::SeqCst)
    }

    /// Streams the client closed before the mock finished writing them.
    pub fn aborted_streams(&self) -> usize {
        self.state.aborted_streams.load(Ordering::SeqCst)
    }

    /// Streams the mock wrote to the end.
    pub fn finished_streams(&self) -> usize {
        self.state.finished_streams.load(Ordering::SeqCst)
    }
}

async fn serve(mut socket: TcpStream, connection: usize, state: Arc<State>) {
    let mut buffer = Vec::new();
    loop {
        let Some(request) = read_request(&mut socket, &mut buffer, connection).await else {
            return;
        };
        let index = {
            let mut requests = state.requests.lock().expect("lock");
            requests.push(request.clone());
            requests.len() - 1
        };
        match (state.handler)(&request, index) {
            Reply::Full {
                status,
                headers,
                body,
            } => {
                let mut head = format!(
                    "HTTP/1.1 {status} Mock\r\ncontent-length: {}\r\n",
                    body.len()
                );
                for (name, value) in &headers {
                    head.push_str(&format!("{name}: {value}\r\n"));
                }
                head.push_str("\r\n");
                if socket.write_all(head.as_bytes()).await.is_err()
                    || socket.write_all(&body).await.is_err()
                {
                    return;
                }
            }
            Reply::Stream {
                status,
                headers,
                chunks,
            } => {
                let mut head = format!("HTTP/1.1 {status} Mock\r\nconnection: close\r\n");
                for (name, value) in &headers {
                    head.push_str(&format!("{name}: {value}\r\n"));
                }
                head.push_str("\r\n");
                if socket.write_all(head.as_bytes()).await.is_err() {
                    return;
                }
                let mut probe = [0u8; 64];
                for (delay, chunk) in chunks {
                    // While waiting, notice a client that closed the connection.
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
                        read = socket.read(&mut probe) => {
                            if matches!(read, Ok(0) | Err(_)) {
                                state.aborted_streams.fetch_add(1, Ordering::SeqCst);
                                return;
                            }
                        }
                    }
                    if socket.write_all(&chunk).await.is_err() || socket.flush().await.is_err() {
                        state.aborted_streams.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                }
                state.finished_streams.fetch_add(1, Ordering::SeqCst);
                let _ = socket.shutdown().await;
                return;
            }
            Reply::Drop => return,
            Reply::Hang => {
                let mut probe = [0u8; 64];
                while matches!(socket.read(&mut probe).await, Ok(n) if n > 0) {}
                return;
            }
            Reply::StallBody { status, body } => {
                let head = format!(
                    "HTTP/1.1 {status} Mock\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                    body.len() + 1000
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.flush().await;
                let mut probe = [0u8; 64];
                while matches!(socket.read(&mut probe).await, Ok(n) if n > 0) {}
                return;
            }
        }
    }
}

/// Reads one request; `None` when the client closed the connection.
async fn read_request(
    socket: &mut TcpStream,
    buffer: &mut Vec<u8>,
    connection: usize,
) -> Option<Recorded> {
    let head_end = loop {
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break position;
        }
        let mut chunk = [0u8; 8192];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or("").split(' ');
    let method = request_line.next().unwrap_or("").to_owned();
    let target = request_line.next().unwrap_or("").to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect();
    let length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    let body_start = head_end + 4;
    while buffer.len() < body_start + length {
        let mut chunk = [0u8; 8192];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    }
    let body = buffer[body_start..body_start + length].to_vec();
    buffer.drain(..body_start + length);
    Some(Recorded {
        method,
        target,
        headers,
        body,
        connection,
    })
}
