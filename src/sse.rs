//! Server-sent events: an incremental parser and typed event streams.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use serde::de::DeserializeOwned;

use crate::error::{ApiError, Error, Result};
use crate::response::{snippet, ResponseMeta};

/// One dispatched server-sent event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The event name; `message` when the stream gave none.
    pub event: String,
    /// The data lines joined with `\n`.
    pub data: String,
    /// The last event id seen so far; `None` until the stream sends one.
    pub id: Option<String>,
    /// The reconnection time in milliseconds, when this event's block set it.
    pub retry: Option<u64>,
}

/// An incremental parser of the `text/event-stream` format (WHATWG HTML,
/// "Interpreting an event stream").
///
/// Feed it the bytes of the stream in chunks of any size; it handles LF,
/// CRLF and bare CR line endings, a leading BOM, and UTF-8 sequences, lines
/// and events split across chunks.
///
/// ```
/// use tungo_sdk::sse::SseParser;
///
/// let mut parser = SseParser::new();
/// let mut events = parser.feed(b"event: text\ndata: hel");
/// events.extend(parser.feed(b"lo\n\n"));
/// assert_eq!(events[0].event, "text");
/// assert_eq!(events[0].data, "hello");
/// ```
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: Vec<u8>,
    /// How far `buffer` has been searched for a line ending.
    scanned: usize,
    started: bool,
    data: Vec<String>,
    has_data: bool,
    event: String,
    last_id: Option<String>,
    retry: Option<u64>,
}

impl SseParser {
    /// Creates a parser at the start of a stream.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consumes a chunk and returns the events it completed.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        let mut out = Vec::new();
        self.buffer.extend_from_slice(chunk);
        if !self.started {
            const BOM: &[u8] = b"\xef\xbb\xbf";
            if self.buffer.len() < BOM.len() && BOM.starts_with(&self.buffer) {
                return out; // may still turn out to be a BOM
            }
            if self.buffer.starts_with(BOM) {
                self.buffer.drain(..BOM.len());
            }
            self.started = true;
        }
        let mut start = 0;
        loop {
            let from = start.max(self.scanned);
            let Some(offset) = self.buffer[from..]
                .iter()
                .position(|byte| *byte == b'\r' || *byte == b'\n')
            else {
                self.scanned = self.buffer.len();
                break;
            };
            let end = from + offset;
            let mut next = end + 1;
            if self.buffer[end] == b'\r' {
                if next == self.buffer.len() {
                    // A CR at the end of a chunk may be the first half of CRLF.
                    self.scanned = end;
                    break;
                }
                if self.buffer[next] == b'\n' {
                    next += 1;
                }
            }
            let line = String::from_utf8_lossy(&self.buffer[start..end]).into_owned();
            self.line(&line, &mut out);
            start = next;
            self.scanned = next;
        }
        self.buffer.drain(..start);
        self.scanned -= start;
        out
    }

    /// Signals the end of the stream. A trailing CR terminates its line; an
    /// event that was not terminated by a blank line is discarded.
    pub fn finish(&mut self) -> Vec<SseEvent> {
        let mut out = Vec::new();
        if self.buffer.last() == Some(&b'\r') {
            let end = self.buffer.len() - 1;
            let line = String::from_utf8_lossy(&self.buffer[..end]).into_owned();
            self.line(&line, &mut out);
        }
        self.buffer.clear();
        self.scanned = 0;
        out
    }

    fn line(&mut self, line: &str, out: &mut Vec<SseEvent>) {
        if line.is_empty() {
            if self.has_data {
                let event = std::mem::take(&mut self.event);
                out.push(SseEvent {
                    event: if event.is_empty() {
                        "message".to_owned()
                    } else {
                        event
                    },
                    data: self.data.join("\n"),
                    id: self.last_id.clone(),
                    retry: self.retry,
                });
            }
            self.data.clear();
            self.has_data = false;
            self.event.clear();
            self.retry = None;
            return;
        }
        if line.starts_with(':') {
            return;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => self.event = value.to_owned(),
            "data" => {
                self.data.push(value.to_owned());
                self.has_data = true;
            }
            "id" if !value.contains('\0') => self.last_id = Some(value.to_owned()),
            "retry" if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
                if let Ok(milliseconds) = value.parse() {
                    self.retry = Some(milliseconds);
                }
            }
            _ => {}
        }
    }
}

/// Parses a complete event stream held in memory.
pub fn parse_sse(bytes: &[u8]) -> Vec<SseEvent> {
    let mut parser = SseParser::new();
    let mut events = parser.feed(bytes);
    events.extend(parser.finish());
    events
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

struct Source {
    response: reqwest::Response,
    parser: SseParser,
    pending: VecDeque<SseEvent>,
    idle_timeout: Duration,
    done: bool,
}

/// Reads the response chunk by chunk and yields events as they complete.
fn raw_events(response: reqwest::Response, idle_timeout: Duration) -> BoxStream<Result<SseEvent>> {
    let source = Source {
        response,
        parser: SseParser::new(),
        pending: VecDeque::new(),
        idle_timeout,
        done: false,
    };
    Box::pin(futures_util::stream::unfold(
        source,
        |mut source| async move {
            loop {
                if let Some(event) = source.pending.pop_front() {
                    return Some((Ok(event), source));
                }
                if source.done {
                    return None;
                }
                match tokio::time::timeout(source.idle_timeout, source.response.chunk()).await {
                    Ok(Ok(Some(chunk))) => source.pending.extend(source.parser.feed(&chunk)),
                    Ok(Ok(None)) => {
                        source.pending.extend(source.parser.finish());
                        source.done = true;
                    }
                    Ok(Err(error)) => {
                        source.done = true;
                        return Some((Err(Error::from_reqwest(error)), source));
                    }
                    Err(_) => {
                        source.done = true;
                        let message =
                            format!("the stream sent no data for {:?}", source.idle_timeout);
                        return Some((Err(Error::timeout(message, None)), source));
                    }
                }
            }
        },
    ))
}

/// A stream of raw server-sent events, including heart-beats.
///
/// Dropping the stream closes the connection.
pub struct RawEventStream {
    events: BoxStream<Result<SseEvent>>,
    meta: ResponseMeta,
}

impl RawEventStream {
    /// The status and headers of the streaming response.
    pub fn response(&self) -> &ResponseMeta {
        &self.meta
    }

    /// Closes the connection. Dropping the stream does the same.
    pub fn close(self) {}
}

impl Stream for RawEventStream {
    type Item = Result<SseEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.as_mut().poll_next(cx)
    }
}

impl std::fmt::Debug for RawEventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawEventStream")
            .field("status", &self.meta.status())
            .finish_non_exhaustive()
    }
}

/// A stream of typed events decoded from server-sent events.
///
/// Events arrive as the server sends them. Heart-beats (`ping` events and
/// events with empty data) are skipped. Dropping the stream, or leaving the
/// loop that reads it, closes the connection.
///
/// ```no_run
/// use futures_util::StreamExt;
/// use tungo_sdk::types::{ChatEvent, ChatMessageRequest};
/// use tungo_sdk::Tungo;
///
/// # async fn run(client: Tungo) -> tungo_sdk::Result<()> {
/// let request = ChatMessageRequest {
///     message: Some("What is the VAT for October?".to_owned()),
///     ..Default::default()
/// };
/// let mut stream = client.chat().create_stream(&request).await?;
/// while let Some(event) = stream.next().await {
///     if let ChatEvent::Text(text) = event? {
///         print!("{}", text.delta);
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct EventStream<T> {
    raw: RawEventStream,
    last_event_id: Option<String>,
    /// Whether an `error` event is a variant of `T` (chat events) rather
    /// than a failure of the stream.
    error_is_event: bool,
    /// Set once an `error` event ended the stream.
    failed: bool,
    marker: PhantomData<fn() -> T>,
}

/// Where the events of a stream come from.
pub(crate) enum StreamBody {
    /// The open response of the server.
    Live(reqwest::Response),
    /// A complete event stream held in memory: the events of a chat turn
    /// replayed by Idempotency-Key.
    Replayed(Vec<u8>),
}

impl<T> EventStream<T> {
    pub(crate) fn new(body: StreamBody, meta: ResponseMeta, idle_timeout: Duration) -> Self {
        let events = match body {
            StreamBody::Live(response) => raw_events(response, idle_timeout),
            StreamBody::Replayed(bytes) => {
                let mut parser = SseParser::new();
                let mut events = parser.feed(&bytes);
                events.extend(parser.finish());
                let events: BoxStream<Result<SseEvent>> =
                    Box::pin(futures_util::stream::iter(events.into_iter().map(Ok)));
                events
            }
        };
        EventStream {
            raw: RawEventStream { events, meta },
            last_event_id: None,
            error_is_event: true,
            failed: false,
            marker: PhantomData,
        }
    }

    /// Makes an `error` event a failure of the stream instead of an event:
    /// for streams whose items have no `error` variant (the feed). The event
    /// is returned once as [`Error::Stream`] with its code and message, and
    /// the stream ends: it did not finish, so reconnect to resume it.
    pub(crate) fn error_ends_stream(mut self) -> Self {
        self.error_is_event = false;
        self
    }

    /// The status and headers of the streaming response.
    pub fn response(&self) -> &ResponseMeta {
        &self.raw.meta
    }

    /// The id of the last event read so far (the value to send as
    /// `Last-Event-ID` to resume a stream).
    pub fn last_event_id(&self) -> Option<&str> {
        self.last_event_id.as_deref()
    }

    /// Gives up typed decoding and returns the raw events, heart-beats included.
    pub fn into_raw(self) -> RawEventStream {
        self.raw
    }

    /// Closes the connection. Dropping the stream does the same.
    pub fn close(self) {}
}

impl<T: DeserializeOwned> Stream for EventStream<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.failed {
                return Poll::Ready(None);
            }
            let event = match self.raw.events.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(event))) => event,
            };
            if event.id.is_some() {
                self.last_event_id.clone_from(&event.id);
            }
            if !self.error_is_event && event.event == "error" && !event.data.is_empty() {
                self.failed = true;
                let data = serde_json::from_str::<serde_json::Value>(&event.data)
                    .unwrap_or_else(|_| serde_json::Value::String(event.data.clone()));
                let meta = &self.raw.meta;
                let error = ApiError::from_event(meta.status(), meta.headers(), &data);
                return Poll::Ready(Some(Err(Error::Stream(Box::new(error)))));
            }
            if let Some(item) = decode_event(&event, &self.raw.meta) {
                return Poll::Ready(Some(item));
            }
        }
    }
}

impl<T> std::fmt::Debug for EventStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStream")
            .field("status", &self.raw.meta.status())
            .field("last_event_id", &self.last_event_id)
            .finish_non_exhaustive()
    }
}

/// Decodes the JSON data of one event; `None` for a heart-beat.
pub fn decode_event<T: DeserializeOwned>(
    event: &SseEvent,
    meta: &ResponseMeta,
) -> Option<Result<T>> {
    if event.event == "ping" || event.data.is_empty() {
        return None;
    }
    Some(
        serde_json::from_str::<T>(&event.data).map_err(|error| Error::Decode {
            message: format!(
                "the data of event `{}` is not the expected JSON: {error}",
                event.event
            ),
            status: Some(meta.status()),
            request_id: meta.request_id().map(str::to_owned),
            body: snippet(event.data.as_bytes()),
            source: Some(Box::new(error)),
        }),
    )
}
