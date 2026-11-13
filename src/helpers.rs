//! Convenience helpers built on the generated resources: one-call chat,
//! chat stream accumulation, long-running waits and incoming hooks.

use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures_util::StreamExt;
use reqwest::Method;
use serde::Serialize;
use serde_json::{json, Value};

use crate::client::Tungo;
use crate::error::{ApiError, Error, Result};
use crate::request::{Auth, Request};
use crate::response::{Response, ResponseMeta};
use crate::sse::EventStream;
use crate::types::{
    AutomationHookAccepted, AutomationRunDetail, AutomationRunDetailState,
    AutomationScenarioRunInput, BankStatementJobRequest, BatchJob, ChatEvent, ChatMessageRequest,
    ChatTurnResult, ChatUsage, WebhookEvent,
};
use crate::webhooks::{
    hook_signature_header, verify_webhook, WebhookHeaders, WebhookVerificationError,
};

/// The result of one chat turn folded from its event stream.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ChatTurn {
    /// The dialog the turn belongs to (response header `X-Dialog-Id`).
    pub dialog_id: Option<String>,
    /// The number of the turn inside the dialog (response header `X-Turn-No`).
    pub turn_no: Option<u64>,
    /// The final answer: `done.answer` when the server sent it, otherwise
    /// the text deltas without the intermediate narration that `progress`
    /// events replaced.
    pub text: String,
    /// `done.stop_reason`; `None` when the turn had no `done` event or it
    /// carried no reason.
    pub stop_reason: Option<String>,
    /// Whether the `done` event arrived. Without it the turn is not
    /// complete: the stream was cut, and `text` is only what came through.
    pub done: bool,
    /// How many events could not be decoded and were left out of `events`.
    pub skipped_events: usize,
    /// `done.cost_tiyn`.
    pub cost_tiyn: Option<i64>,
    /// `done.usage`.
    pub usage: Option<ChatUsage>,
    /// `done.choice_id`: set when the turn waits for the user's answer.
    pub choice_id: Option<String>,
    /// Every event of the turn in order, heart-beats excluded.
    pub events: Vec<ChatEvent>,
    /// The first `error` event of the turn, if there was one.
    pub error: Option<Box<ApiError>>,
}

/// Folds the events of one chat turn into a [`ChatTurn`].
///
/// [`EventStream::accumulate`] does this for a whole stream; use the
/// accumulator directly to show events as they arrive and still get the
/// final result.
///
/// ```no_run
/// use futures_util::StreamExt;
/// use tungo_sdk::ChatAccumulator;
///
/// # async fn run(client: tungo_sdk::Tungo) -> tungo_sdk::Result<()> {
/// let mut stream = client.ask_stream("What is the VAT for October?").await?;
/// let mut turn = ChatAccumulator::new(stream.response());
/// while let Some(event) = stream.next().await {
///     turn.push(event?);
/// }
/// println!("{}", turn.finish().text);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ChatAccumulator {
    meta: ResponseMeta,
    segments: Vec<(Option<f64>, String)>,
    answer: Option<String>,
    turn: ChatTurn,
}

impl ChatAccumulator {
    /// Starts a turn; the dialog id and the turn number are read from the
    /// headers of the streaming response.
    pub fn new(response: &ResponseMeta) -> Self {
        let turn = ChatTurn {
            dialog_id: response.header("x-dialog-id").map(str::to_owned),
            turn_no: response
                .header("x-turn-no")
                .and_then(|value| value.trim().parse().ok()),
            ..ChatTurn::default()
        };
        ChatAccumulator {
            meta: response.clone(),
            segments: Vec::new(),
            answer: None,
            turn,
        }
    }

    /// Adds the next event of the turn.
    pub fn push(&mut self, event: ChatEvent) {
        match &event {
            ChatEvent::Text(text) => self.segments.push((text.step, text.delta.clone())),
            ChatEvent::Progress(progress) => {
                // The narration of a finished step is not part of the answer.
                self.segments
                    .retain(|(step, _)| *step != Some(progress.step));
            }
            ChatEvent::Error(_) if self.turn.error.is_none() => {
                let raw = serde_json::to_value(&event).unwrap_or(Value::Null);
                let error = ApiError::from_event(self.meta.status(), self.meta.headers(), &raw);
                self.turn.error = Some(Box::new(error));
            }
            ChatEvent::Done(done) => {
                self.turn.done = true;
                self.answer.clone_from(&done.answer);
                self.turn.stop_reason.clone_from(&done.stop_reason);
                self.turn.cost_tiyn = done.cost_tiyn;
                self.turn.usage.clone_from(&done.usage);
                self.turn.choice_id.clone_from(&done.choice_id);
            }
            // An event the models could not hold is kept as raw JSON; the end
            // of the turn and its failure must not be missed because of that.
            ChatEvent::Unknown(raw) => match raw.get("type").and_then(Value::as_str) {
                Some("done") => {
                    let text = |name: &str| raw.get(name).and_then(Value::as_str);
                    self.turn.done = true;
                    self.answer = text("answer").map(str::to_owned);
                    self.turn.stop_reason = text("stop_reason").map(str::to_owned);
                    self.turn.cost_tiyn = raw.get("cost_tiyn").and_then(Value::as_i64);
                    self.turn.choice_id = text("choice_id").map(str::to_owned);
                }
                Some("error") if self.turn.error.is_none() => {
                    let error = ApiError::from_event(self.meta.status(), self.meta.headers(), raw);
                    self.turn.error = Some(Box::new(error));
                }
                _ => {}
            },
            _ => {}
        }
        self.turn.events.push(event);
    }

    /// Notes an event of the stream that could not be decoded: it is counted
    /// in [`ChatTurn::skipped_events`] and otherwise left out.
    pub fn skip(&mut self) {
        self.turn.skipped_events += 1;
    }

    /// Finishes the turn. The `error` field holds the first `error` event;
    /// check [`ChatTurn::done`] to know whether the turn was complete.
    pub fn finish(mut self) -> ChatTurn {
        self.turn.text = match self.answer.take() {
            Some(answer) => answer,
            None => self
                .segments
                .iter()
                .map(|(_, delta)| delta.as_str())
                .collect(),
        };
        self.turn
    }
}

impl EventStream<ChatEvent> {
    /// The dialog of the turn (response header `X-Dialog-Id`).
    pub fn dialog_id(&self) -> Option<&str> {
        self.response().header("x-dialog-id")
    }

    /// The number of the turn (response header `X-Turn-No`).
    pub fn turn_no(&self) -> Option<u64> {
        self.response()
            .header("x-turn-no")
            .and_then(|value| value.trim().parse().ok())
    }

    /// Reads the stream to its end and returns the folded turn.
    ///
    /// - An `error` event is returned as [`Error::Stream`].
    /// - A stream that ends without its `done` event is not a finished turn:
    ///   it is an [`Error::Decode`] ("the stream ended before completion"),
    ///   never a partial text returned as the answer.
    /// - An event whose data cannot be decoded is skipped and counted in
    ///   [`ChatTurn::skipped_events`]; the turn goes on.
    /// - A transport error or an idle timeout ends the turn with that error.
    pub async fn accumulate(mut self) -> Result<ChatTurn> {
        let mut accumulator = ChatAccumulator::new(self.response());
        while let Some(event) = self.next().await {
            match event {
                Ok(event) => accumulator.push(event),
                Err(Error::Decode { .. }) => accumulator.skip(),
                Err(error) => return Err(error),
            }
        }
        let mut turn = accumulator.finish();
        if let Some(error) = turn.error.take() {
            return Err(Error::Stream(error));
        }
        if !turn.done {
            let meta = self.response();
            return Err(Error::Decode {
                message: "the stream ended before completion (no `done` event)".to_owned(),
                status: Some(meta.status()),
                request_id: meta.request_id().map(str::to_owned),
                body: String::new(),
                source: None,
            });
        }
        Ok(turn)
    }
}

/// How long a wait helper polls, and how often.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WaitOptions {
    /// The overall deadline (10 minutes by default).
    pub timeout: Duration,
    /// The delay before the second poll (1 s by default).
    pub initial_interval: Duration,
    /// The longest delay between polls (5 s by default).
    pub max_interval: Duration,
}

impl Default for WaitOptions {
    fn default() -> Self {
        WaitOptions {
            timeout: Duration::from_secs(600),
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(5),
        }
    }
}

impl WaitOptions {
    /// The default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the overall deadline.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the first and the longest delay between polls.
    #[must_use]
    pub fn poll_interval(mut self, initial: Duration, max: Duration) -> Self {
        self.initial_interval = initial;
        self.max_interval = max.max(initial);
        self
    }
}

/// Sleeps between polls with a growing interval and enforces the deadline.
struct Poller {
    started: Instant,
    interval: Duration,
    options: WaitOptions,
}

impl Poller {
    fn new(options: &WaitOptions) -> Self {
        Poller {
            started: Instant::now(),
            interval: options.initial_interval,
            options: options.clone(),
        }
    }

    async fn pause(&mut self, what: &str) -> Result<()> {
        let elapsed = self.started.elapsed();
        if elapsed >= self.options.timeout {
            return Err(Error::timeout(
                format!("{what} did not finish within {:?}", self.options.timeout),
                None,
            ));
        }
        tokio::time::sleep(self.interval.min(self.options.timeout - elapsed)).await;
        self.interval = self.interval.mul_f64(1.5).min(self.options.max_interval);
        Ok(())
    }
}

/// The body of an incoming-hook request (`POST /hooks/{token}`).
#[derive(Clone)]
pub struct HookPayload {
    body: Vec<u8>,
    content_type: String,
    secret: Option<String>,
    signature_header: Option<String>,
    event_id: Option<String>,
}

impl std::fmt::Debug for HookPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookPayload")
            .field("bytes", &self.body.len())
            .field("content_type", &self.content_type)
            .field("secret", &self.secret.as_ref().map(|_| "[redacted]"))
            .field("signature_header", &self.signature_header)
            .field("event_id", &self.event_id)
            .finish()
    }
}

impl HookPayload {
    /// A JSON payload.
    pub fn json<T: Serialize + ?Sized>(payload: &T) -> Result<Self> {
        let body = serde_json::to_vec(payload)
            .map_err(|e| Error::Validation(format!("cannot serialize the hook payload: {e}")))?;
        Ok(Self::bytes(body, "application/json"))
    }

    /// A payload of arbitrary bytes with the given content type.
    pub fn bytes(body: impl Into<Vec<u8>>, content_type: impl Into<String>) -> Self {
        HookPayload {
            body: body.into(),
            content_type: content_type.into(),
            secret: None,
            signature_header: None,
            event_id: None,
        }
    }

    /// Signs the payload: the request carries `X-Signature: sha256=<hex>`
    /// computed over the exact bytes sent (or the header set with
    /// [`HookPayload::signature_header`]).
    #[must_use]
    pub fn secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }

    /// The header the hook reads its signature from: the `signature_header`
    /// it was created with. `X-Signature` when not set.
    #[must_use]
    pub fn signature_header(mut self, name: impl Into<String>) -> Self {
        self.signature_header = Some(name.into());
        self
    }

    /// Sets the `X-Event-Id` header, which the API uses to drop duplicates.
    /// It is also the `Idempotency-Key` unless the request options carry one.
    #[must_use]
    pub fn event_id(mut self, event_id: impl Into<String>) -> Self {
        self.event_id = Some(event_id.into());
        self
    }

    /// The exact bytes that will be sent.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub(crate) fn into_request(self, token: &str) -> Result<Request> {
        let mut request = Request::new(Method::POST, "/hooks/{token}");
        request.path_param("token", token)?;
        request.auth(Auth::None);
        request.idempotent();
        let header = self.signature_header.as_deref().unwrap_or("X-Signature");
        if header.is_empty()
            || header.len() > 64
            || !header
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(Error::Validation(
                "the signature header must be 1 to 64 Latin letters, digits or hyphens, like the hook's signature_header".into(),
            ));
        }
        if let Some(secret) = &self.secret {
            let signature = hook_signature_header(&self.body, secret)
                .map_err(|e| Error::Validation(e.to_string()))?;
            request.header(header, &signature)?;
        }
        if let Some(event_id) = &self.event_id {
            request.header("X-Event-Id", event_id)?;
            // The event id is the default `Idempotency-Key`: the API reads that
            // header first, so a random key would override the event id.
            request.default_key = Some(event_id.clone());
        }
        request.bytes(self.body, &self.content_type);
        Ok(request)
    }
}

impl Tungo {
    /// Asks the AI accountant one question and waits for the whole answer.
    ///
    /// A turn that finished with an error is returned as [`Error::Stream`].
    /// Use [`Tungo::ask_with`] to continue a dialog or attach files.
    pub async fn ask(&self, message: impl Into<String>) -> Result<Response<ChatTurnResult>> {
        let request = ChatMessageRequest {
            message: Some(message.into()),
            ..ChatMessageRequest::default()
        };
        self.ask_with(&request).await
    }

    /// Like [`Tungo::ask`], with a full chat request.
    pub async fn ask_with(&self, request: &ChatMessageRequest) -> Result<Response<ChatTurnResult>> {
        let response = self.chat().create(request).await?;
        if let Some(turn_error) = response.errors.first() {
            let event = json!({
                "type": "error",
                "error": turn_error,
                "request_id": response.request_id(),
            });
            let error = ApiError::from_event(response.status(), response.headers(), &event);
            return Err(Error::Stream(Box::new(error)));
        }
        Ok(response)
    }

    /// Asks the AI accountant one question and returns the answer as a
    /// stream of events. Call [`EventStream::accumulate`] to fold it.
    pub async fn ask_stream(&self, message: impl Into<String>) -> Result<EventStream<ChatEvent>> {
        let request = ChatMessageRequest {
            message: Some(message.into()),
            ..ChatMessageRequest::default()
        };
        self.chat().create_stream(&request).await
    }

    /// Polls an automation run until it reaches a final state (`done`,
    /// `error`, `skipped` or `cancelled`).
    ///
    /// A state this version of the SDK does not know is not final: the run
    /// is polled again. Fails with [`Error::Timeout`] when the deadline
    /// passes first, and with [`Error::Decode`] when an answer has no state.
    pub async fn wait_for_run(
        &self,
        run_id: &str,
        options: &WaitOptions,
    ) -> Result<Response<AutomationRunDetail>> {
        let mut poller = Poller::new(options);
        loop {
            let run = self.automations().get_run(run_id).await?;
            match &run.state {
                AutomationRunDetailState::Done
                | AutomationRunDetailState::Error
                | AutomationRunDetailState::Skipped
                | AutomationRunDetailState::Cancelled => return Ok(run),
                AutomationRunDetailState::Unknown(state) if state.is_empty() => {
                    return Err(Error::Decode {
                        message: "the run has no state".to_owned(),
                        status: Some(run.status()),
                        request_id: run.request_id().map(str::to_owned),
                        body: String::new(),
                        source: None,
                    })
                }
                // Queued, running, or a state added by a newer API: not final.
                _ => {}
            }
            poller.pause(&format!("run {run_id}")).await?;
        }
    }

    /// Starts a scenario and waits until its run reaches a final state.
    pub async fn run_scenario_and_wait(
        &self,
        scenario_id: &str,
        input: Option<&AutomationScenarioRunInput>,
        options: &WaitOptions,
    ) -> Result<Response<AutomationRunDetail>> {
        let started = self.automations().run_scenario(scenario_id, input).await?;
        self.wait_for_run(&started.id, options).await
    }

    /// Polls a batch job until it stops running.
    ///
    /// Fails with [`Error::Timeout`] when the deadline passes first, and
    /// with [`Error::Decode`] when an answer is not a batch job (a job
    /// without its `running` flag is never taken for a finished one).
    pub async fn wait_for_batch(
        &self,
        batch_id: &str,
        options: &WaitOptions,
    ) -> Result<Response<BatchJob>> {
        let mut poller = Poller::new(options);
        loop {
            let job = self.batches().get(batch_id).await?;
            if !job.running {
                return Ok(job);
            }
            poller.pause(&format!("batch {batch_id}")).await?;
        }
    }

    /// Uploads a bank statement file for processing into proposals.
    pub async fn upload_bank_statement(
        &self,
        file_name: &str,
        content: &[u8],
    ) -> Result<Response<BatchJob>> {
        let request = BankStatementJobRequest {
            file: Some(STANDARD.encode(content)),
            file_name: Some(file_name.to_owned()),
            ..BankStatementJobRequest::default()
        };
        self.batches().create_bank_statement(&request).await
    }

    /// Posts an event to an incoming hook. The API key is not sent; the
    /// payload is signed when it carries a secret. This is the same call as
    /// `client.automations().post_hook(token, payload)`.
    pub async fn send_hook(
        &self,
        token: &str,
        payload: HookPayload,
    ) -> Result<Response<AutomationHookAccepted>> {
        self.automations().post_hook(token, payload).await
    }

    /// Verifies a webhook with the client's webhook secret and returns the
    /// decoded event. `body` must be the raw request body.
    pub fn verify_webhook<H: WebhookHeaders + ?Sized>(
        &self,
        body: impl AsRef<[u8]>,
        headers: &H,
    ) -> std::result::Result<WebhookEvent, WebhookVerificationError> {
        let secret = self
            .webhook_secret()
            .ok_or(WebhookVerificationError::MissingSecret)?;
        verify_webhook(body, headers, secret)
    }
}
