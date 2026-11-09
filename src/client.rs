//! Клиент, иерархия ошибок, SSE-парсер, денежные хелперы и высокоуровневые методы для новичков.

use crate::crypto::sign_hook_payload;
use crate::json::JsonValue;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TungoErrorKind {
    Validation,      // 400
    Auth,            // 401
    PaymentRequired, // 402
    Permission,      // 403
    NotFound,        // 404
    Conflict,        // 409
    RateLimit,       // 429
    Server,          // 5xx
    Local,           // клиентская/сетевая ошибка
}

#[derive(Debug, Clone, PartialEq)]
pub struct TungoError {
    pub kind: TungoErrorKind,
    pub status: u16,
    pub code: String,
    pub message: String,
    pub hint: Option<String>,
    pub request_id: Option<String>,
    pub retry_after: Option<f64>,
}

impl TungoError {
    pub fn local(msg: &str) -> Self {
        Self {
            kind: TungoErrorKind::Local,
            status: 0,
            code: "local_error".to_string(),
            message: msg.to_string(),
            hint: None,
            request_id: None,
            retry_after: None,
        }
    }
}

impl std::fmt::Display for TungoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TungoError({} {}): {}", self.status, self.code, self.message)
    }
}

impl std::error::Error for TungoError {}

pub fn parse_api_error(status: u16, raw_body: &str, headers: &BTreeMap<String, String>) -> TungoError {
    let kind = match status {
        400 => TungoErrorKind::Validation,
        401 => TungoErrorKind::Auth,
        402 => TungoErrorKind::PaymentRequired,
        403 => TungoErrorKind::Permission,
        404 => TungoErrorKind::NotFound,
        409 => TungoErrorKind::Conflict,
        429 => TungoErrorKind::RateLimit,
        _ => TungoErrorKind::Server,
    };
    let mut code = "api_error".to_string();
    let mut message = raw_body.trim().to_string();
    let mut hint = None;
    let mut request_id = None;

    if let Ok(val) = JsonValue::parse(raw_body) {
        if let Some(r) = val.get("request_id").and_then(|v| v.as_str()) {
            request_id = Some(r.to_string());
        }
        if let Some(h) = val.get("hint").and_then(|v| v.as_str()) {
            hint = Some(h.to_string());
        }
        let err_obj = val.get("error").unwrap_or(&val);
        if let Some(c) = err_obj.get("code").and_then(|v| v.as_str()) {
            code = c.to_string();
        }
        if let Some(m) = err_obj.get("message").and_then(|v| v.as_str()) {
            message = m.to_string();
        }
        if let Some(h) = err_obj.get("hint").and_then(|v| v.as_str()) {
            hint = Some(h.to_string());
        }
        if let Some(r) = err_obj.get("request_id").and_then(|v| v.as_str()) {
            request_id = Some(r.to_string());
        }
    }
    let retry_after = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("retry-after"))
        .and_then(|(_, v)| v.parse::<f64>().ok());

    TungoError {
        kind,
        status,
        code,
        message,
        hint,
        request_id,
        retry_after,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitInfo {
    pub limit: i64,
    pub remaining: i64,
    pub reset: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SSEEvent {
    pub event: String,
    pub data: JsonValue,
    pub raw: String,
}

/// Разбирает поток text/event-stream (SSE) в вектор событий SSEEvent.
pub fn parse_sse(raw_stream: &str) -> Vec<SSEEvent> {
    let mut out = Vec::new();
    let mut event_type = String::new();
    let mut data_lines: Vec<String> = Vec::new();

    let flush = |ev: &mut String, lines: &mut Vec<String>, out: &mut Vec<SSEEvent>| {
        if ev.is_empty() && lines.is_empty() {
            return;
        }
        let raw = lines.join("\n");
        let e_name = if ev.is_empty() { "message".to_string() } else { std::mem::take(ev) };
        lines.clear();
        let data = JsonValue::parse(&raw).unwrap_or_else(|_| JsonValue::String(raw.clone()));
        out.push(SSEEvent {
            event: e_name,
            data,
            raw,
        });
    };

    for line in raw_stream.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            flush(&mut event_type, &mut data_lines, &mut out);
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event_type = rest.trim().to_string();
        } else if let Some(rest) = line.strip_prefix("data:") {
            let val = rest.strip_prefix(' ').unwrap_or(rest);
            data_lines.push(val.to_string());
        }
    }
    flush(&mut event_type, &mut data_lines, &mut out);
    out
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatStreamAccumulated {
    pub text: String,
    pub dialog_id: String,
    pub stop_reason: String,
    pub cost_tiyn: i64,
}

/// Собирает вектор событий SSE в итоговый ответ ChatStreamAccumulated.
pub fn accumulate_chat_stream(events: &[SSEEvent]) -> ChatStreamAccumulated {
    let mut text = String::new();
    let mut dialog_id = String::new();
    let mut stop_reason = String::new();
    let mut cost_tiyn = 0i64;
    for ev in events {
        match ev.event.as_str() {
            "meta" => {
                if let Some(d) = ev.data.get("dialog_id").and_then(|v| v.as_str()) {
                    dialog_id = d.to_string();
                }
            }
            "text" | "delta" | "message" => {
                if let Some(d) = ev.data.get("delta").and_then(|v| v.as_str()) {
                    text.push_str(d);
                } else if let Some(t) = ev.data.get("text").and_then(|v| v.as_str()) {
                    text.push_str(t);
                } else if let Some(s) = ev.data.as_str() {
                    text.push_str(s);
                }
            }
            "done" => {
                if let Some(d) = ev.data.get("dialog_id").and_then(|v| v.as_str()) {
                    dialog_id = d.to_string();
                }
                if let Some(sr) = ev.data.get("stop_reason").and_then(|v| v.as_str()) {
                    stop_reason = sr.to_string();
                }
                if let Some(ct) = ev.data.get("cost_tiyn").and_then(|v| v.as_i64()) {
                    cost_tiyn = ct;
                }
                if text.is_empty() {
                    if let Some(ans) = ev.data.get("answer").and_then(|v| v.as_str()) {
                        text = ans.to_string();
                    }
                }
            }
            _ => {}
        }
    }
    ChatStreamAccumulated {
        text,
        dialog_id,
        stop_reason,
        cost_tiyn,
    }
}

/// Проверяет контрольную сумму 12-значного БИН или ИИН Республики Казахстан (модуль 11, два прохода).
pub fn is_valid_bin_iin(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 12 || !bytes.iter().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let digits: Vec<u32> = bytes.iter().map(|b| (b - b'0') as u32).collect();
    let w1: [u32; 11] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
    let sum1: u32 = (0..11).map(|i| digits[i] * w1[i]).sum();
    let mut control = sum1 % 11;
    if control == 10 {
        let w2: [u32; 11] = [3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2];
        let sum2: u32 = (0..11).map(|i| digits[i] * w2[i]).sum();
        control = sum2 % 11;
    }
    control < 10 && control == digits[11]
}

/// Утилиты для работы с деньгами в тиынах (1 тенге = 100 тиын).
pub struct TungoMoney;

impl TungoMoney {
    pub fn to_tiyn(amount: f64) -> i64 {
        (amount * 100.0).round() as i64
    }

    pub fn to_tenge(tiyn: i64) -> f64 {
        (tiyn as f64) / 100.0
    }

    pub fn format_tiyn(tiyn: i64, currency: &str) -> String {
        let symbol = match currency.trim().to_ascii_uppercase().as_str() {
            "" | "KZT" => "₸",
            "USD" => "$",
            "EUR" => "€",
            "RUB" => "₽",
            other => return format!("{} {}", tiyn, other),
        };
        let neg = tiyn < 0;
        let abs = tiyn.abs();
        let whole = abs / 100;
        let frac = abs % 100;
        let whole_str = whole.to_string();
        let mut groups = Vec::new();
        let mut end = whole_str.len();
        while end > 3 {
            groups.push(&whole_str[end - 3..end]);
            end -= 3;
        }
        groups.push(&whole_str[0..end]);
        groups.reverse();
        let sign = if neg { "-" } else { "" };
        format!("{}{},{:02} {}", sign, groups.join(" "), frac, symbol)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WebhookEnvelope {
    pub event: String,
    pub timestamp: String,
    pub company_id: String,
    pub data: JsonValue,
}

#[derive(Debug, Clone)]
pub struct RequestSpec {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<JsonValue>,
    pub raw_body: Option<String>,
    pub auth: String,
    pub idempotent: bool,
    pub idempotency_key: Option<String>,
    pub retry_safe: bool,
    pub force_stream: bool,
}

#[derive(Debug, Clone)]
pub struct AskInput {
    pub prompt: String,
    pub company_id: Option<String>,
    pub conversation_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AskOutput {
    pub conversation_id: String,
    pub message_id: String,
    pub reply: String,
    pub raw: JsonValue,
}

#[derive(Debug, Clone)]
pub struct WaitOptions {
    pub company_id: Option<String>,
    pub poll_interval_ms: u64,
    pub timeout_ms: u64,
}

impl Default for WaitOptions {
    fn default() -> Self {
        Self {
            company_id: None,
            poll_interval_ms: 500,
            timeout_ms: 60_000,
        }
    }
}

pub struct TungoClientBuilder {
    api_key: Option<String>,
    base_url: String,
    workspace_id: Option<String>,
    company_id: Option<String>,
    max_retries: usize,
}

impl TungoClientBuilder {
    pub fn new() -> Self {
        Self {
            api_key: std::env::var("TUNGO_API_KEY").ok(),
            base_url: std::env::var("TUNGO_BASE_URL")
                .unwrap_or_else(|_| "https://api.tungo.kz".to_string()),
            workspace_id: std::env::var("TUNGO_WORKSPACE_ID").ok(),
            company_id: std::env::var("TUNGO_COMPANY_ID").ok(),
            max_retries: 2,
        }
    }
    pub fn api_key(mut self, v: impl Into<String>) -> Self {
        self.api_key = Some(v.into());
        self
    }
    pub fn base_url(mut self, v: impl Into<String>) -> Self {
        self.base_url = v.into().trim_end_matches('/').to_string();
        self
    }
    pub fn workspace_id(mut self, v: impl Into<String>) -> Self {
        self.workspace_id = Some(v.into());
        self
    }
    pub fn company_id(mut self, v: impl Into<String>) -> Self {
        self.company_id = Some(v.into());
        self
    }
    pub fn max_retries(mut self, n: usize) -> Self {
        self.max_retries = n;
        self
    }
    pub fn build(self) -> TungoClient {
        TungoClient {
            api_key: self.api_key,
            base_url: self.base_url,
            workspace_id: self.workspace_id,
            company_id: self.company_id,
            max_retries: self.max_retries,
            last_rate_limit: Arc::new(Mutex::new(None)),
        }
    }
}

impl Default for TungoClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TungoClient {
    api_key: Option<String>,
    base_url: String,
    workspace_id: Option<String>,
    company_id: Option<String>,
    max_retries: usize,
    last_rate_limit: Arc<Mutex<Option<RateLimitInfo>>>,
}

impl TungoClient {
    pub fn builder() -> TungoClientBuilder {
        TungoClientBuilder::new()
    }

    pub fn last_rate_limit(&self) -> Option<RateLimitInfo> {
        self.last_rate_limit.lock().ok().and_then(|g| g.clone())
    }

    pub(crate) fn resolve_company_id(&self, explicit: Option<&str>) -> Result<String, TungoError> {
        if let Some(c) = explicit.filter(|s| !s.is_empty()) {
            return Ok(c.to_string());
        }
        if let Some(c) = self.company_id.as_deref().filter(|s| !s.is_empty()) {
            return Ok(c.to_string());
        }
        Err(TungoError::local("не указан company_id"))
    }

    pub(crate) fn resolve_workspace_id(&self, explicit: Option<&str>) -> Result<String, TungoError> {
        if let Some(w) = explicit.filter(|s| !s.is_empty()) {
            return Ok(w.to_string());
        }
        if let Some(w) = self.workspace_id.as_deref().filter(|s| !s.is_empty()) {
            return Ok(w.to_string());
        }
        Err(TungoError::local("не указан workspace_id"))
    }

    /// Быстрый синхронный запрос к ИИ-бухгалтеру в одну строчку (stream = false).
    pub fn ask(&self, input: AskInput) -> Result<AskOutput, TungoError> {
        let mut body = JsonValue::empty_object();
        body.insert("message", JsonValue::String(input.prompt));
        body.insert("stream", JsonValue::Bool(false));
        if let Some(cid) = input.conversation_id {
            body.insert("dialog_id", JsonValue::String(cid.clone()));
            body.insert("conversation_id", JsonValue::String(cid));
        }
        let mut params = BTreeMap::new();
        if let Some(co) = input.company_id {
            params.insert("co".to_string(), co);
        }
        let res = self.chat().create(&body, Some(&params))?;
        let conv_id = res
            .get("dialog_id")
            .or_else(|| res.get("conversation_id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let msg = res.get("assistant_message");
        let msg_id = msg
            .and_then(|m| m.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let reply = res
            .get("answer")
            .and_then(|v| v.as_str())
            .or_else(|| msg.and_then(|m| m.get("text")).and_then(|v| v.as_str()))
            .unwrap_or_default()
            .to_string();
        Ok(AskOutput {
            conversation_id: conv_id,
            message_id: msg_id,
            reply,
            raw: res,
        })
    }

    /// Дождаться завершения запуска сценария автоматизации (state в done / error / cancelled / skipped).
    pub fn wait_for_run(&self, run_id: &str, opts: WaitOptions) -> Result<JsonValue, TungoError> {
        let mut params = BTreeMap::new();
        if let Some(co) = opts.company_id {
            params.insert("co".to_string(), co);
        }
        let start = std::time::Instant::now();
        let timeout = Duration::from_millis(if opts.timeout_ms > 0 { opts.timeout_ms } else { 60_000 });
        let interval = Duration::from_millis(if opts.poll_interval_ms > 0 { opts.poll_interval_ms } else { 500 });
        loop {
            let run = self.automations().get_run(run_id, Some(&params))?;
            if let Some(st) = run.get("state").and_then(|v| v.as_str()) {
                if matches!(st, "done" | "error" | "cancelled" | "skipped") {
                    return Ok(run);
                }
            }
            if start.elapsed() >= timeout {
                return Err(TungoError::local("превышено время ожидания завершения запуска"));
            }
            thread::sleep(interval);
        }
    }

    /// Запустить сценарий автоматизации и дождаться его завершения.
    pub fn run_scenario_and_wait(
        &self,
        scenario_id: &str,
        body: &JsonValue,
        opts: WaitOptions,
    ) -> Result<JsonValue, TungoError> {
        let mut params = BTreeMap::new();
        if let Some(co) = opts.company_id.as_ref() {
            params.insert("co".to_string(), co.clone());
        }
        let started = self.automations().run_scenario(scenario_id, body, Some(&params))?;
        let run_id = started
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TungoError::local("в ответе run_scenario отсутствует id запуска"))?;
        self.wait_for_run(run_id, opts)
    }

    /// Дождаться завершения задания пачки (running == false).
    pub fn wait_for_batch(&self, batch_id: &str, opts: WaitOptions) -> Result<JsonValue, TungoError> {
        let mut params = BTreeMap::new();
        if let Some(co) = opts.company_id {
            params.insert("co".to_string(), co);
        }
        let start = std::time::Instant::now();
        let timeout = Duration::from_millis(if opts.timeout_ms > 0 { opts.timeout_ms } else { 60_000 });
        let interval = Duration::from_millis(if opts.poll_interval_ms > 0 { opts.poll_interval_ms } else { 500 });
        loop {
            let job = self.batches().get(batch_id, Some(&params))?;
            if !job.get("running").and_then(|v| v.as_bool()).unwrap_or(false) {
                return Ok(job);
            }
            if start.elapsed() >= timeout {
                return Err(TungoError::local("превышено время ожидания завершения пачки"));
            }
            thread::sleep(interval);
        }
    }

    /// Загрузить банковскую выписку (1CClientBankExchange .txt или файл в байтах) на разбор в предложения.
    pub fn upload_bank_statement(
        &self,
        file_name: &str,
        content: &[u8],
        is_plain_text: bool,
        company_id: Option<&str>,
    ) -> Result<JsonValue, TungoError> {
        let mut body = JsonValue::empty_object();
        body.insert("file_name", JsonValue::String(file_name.to_string()));
        if is_plain_text {
            body.insert(
                "text",
                JsonValue::String(String::from_utf8_lossy(content).to_string()),
            );
        } else {
            body.insert("file", JsonValue::String(crate::crypto::base64_encode(content)));
        }
        let mut params = BTreeMap::new();
        if let Some(co) = company_id {
            params.insert("co".to_string(), co.to_string());
        }
        self.batches().create_bank_statement(&body, Some(&params))
    }

    /// Отправляет событие во входящий хук /hooks/{token} с автоматической HMAC-подписью X-Signature.
    pub fn send_hook(&self, token: &str, payload: &JsonValue, secret: Option<&str>) -> Result<JsonValue, TungoError> {
        let raw = payload.to_json_string();
        let mut headers = BTreeMap::new();
        if let Some(sec) = secret.filter(|s| !s.is_empty()) {
            headers.insert("X-Signature".to_string(), sign_hook_payload(&raw, sec));
        }
        self.do_json(RequestSpec {
            method: "POST".to_string(),
            path: format!("/hooks/{}", url_encode(token)),
            query: BTreeMap::new(),
            headers,
            body: None,
            raw_body: Some(raw),
            auth: "hook".to_string(),
            idempotent: true,
            idempotency_key: None,
            retry_safe: true,
            force_stream: false,
        })
    }

    pub(crate) fn do_json(&self, spec: RequestSpec) -> Result<JsonValue, TungoError> {
        let raw = self.do_raw(spec, "application/json")?;
        if raw.trim().is_empty() {
            return Ok(JsonValue::Null);
        }
        JsonValue::parse(&raw).map_err(|e| TungoError::local(&e))
    }

    pub(crate) fn do_sse(&self, spec: RequestSpec) -> Result<Vec<SSEEvent>, TungoError> {
        let raw = self.do_raw(spec, "text/event-stream")?;
        Ok(parse_sse(&raw))
    }

    fn do_raw(&self, spec: RequestSpec, accept: &str) -> Result<String, TungoError> {
        let mut body_str = spec.raw_body.clone();
        if body_str.is_none() {
            if let Some(mut b) = spec.body.clone() {
                if spec.force_stream {
                    b.insert("stream", JsonValue::Bool(true));
                }
                body_str = Some(b.to_json_string());
            }
        }
        let idem_key = if spec.idempotent {
            Some(spec.idempotency_key.clone().unwrap_or_else(gen_idempotency_key))
        } else {
            None
        };

        let max_attempts = if spec.retry_safe { 1 + self.max_retries } else { 1 };
        for attempt in 0..max_attempts {
            match self.send_once(&spec, body_str.as_deref(), idem_key.as_deref(), accept) {
                Ok(resp_body) => return Ok(resp_body),
                Err(err) => {
                    let retryable = matches!(err.status, 429 | 502 | 503 | 504);
                    if attempt + 1 < max_attempts && retryable {
                        let wait_ms = err
                            .retry_after
                            .map(|s| (s * 1000.0) as u64)
                            .unwrap_or(200 * (1u64 << attempt));
                        thread::sleep(Duration::from_millis(wait_ms));
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(TungoError::local("превышено число попыток"))
    }

    fn send_once(
        &self,
        spec: &RequestSpec,
        body_str: Option<&str>,
        idem_key: Option<&str>,
        accept: &str,
    ) -> Result<String, TungoError> {
        let mut full_path = spec.path.clone();
        if !spec.query.is_empty() {
            let qs: Vec<String> = spec
                .query
                .iter()
                .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
                .collect();
            full_path.push('?');
            full_path.push_str(&qs.join("&"));
        }

        // Для HTTPS (например, https://api.tungo.kz) в zero-dependency окружении используем системный curl,
        // а для HTTP (локальные тестовые стенды) — прямой TcpStream.
        if self.base_url.starts_with("https://") {
            let full_url = format!("{}{}", self.base_url, full_path);
            let mut cmd = std::process::Command::new("curl");
            cmd.arg("-sS")
                .arg("-i")
                .arg("-X")
                .arg(&spec.method)
                .arg("-H")
                .arg(format!("Accept: {}", accept))
                .arg("-H")
                .arg("User-Agent: tungo-sdk-rust/0.1.0");
            if spec.auth == "key" {
                let key = self
                    .api_key
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| TungoError {
                        kind: TungoErrorKind::Auth,
                        status: 401,
                        code: "missing_api_key".to_string(),
                        message: "API ключ не задан".to_string(),
                        hint: None,
                        request_id: None,
                        retry_after: None,
                    })?;
                cmd.arg("-H").arg(format!("x-api-key: {}", key));
                cmd.arg("-H").arg(format!("Authorization: Bearer {}", key));
            }
            if let Some(ik) = idem_key {
                cmd.arg("-H").arg(format!("Idempotency-Key: {}", ik));
            }
            for (k, v) in &spec.headers {
                cmd.arg("-H").arg(format!("{}: {}", k, v));
            }
            if let Some(b) = body_str {
                cmd.arg("-H").arg("Content-Type: application/json");
                cmd.arg("--data-raw").arg(b);
            }
            cmd.arg(&full_url);
            let out = cmd.output().map_err(|e| TungoError::local(&e.to_string()))?;
            let raw_resp = String::from_utf8_lossy(&out.stdout).to_string();
            let mut reader = BufReader::new(raw_resp.as_bytes());
            return self.read_http_response(&mut reader);
        }

        let url_no_scheme = self
            .base_url
            .strip_prefix("http://")
            .unwrap_or(&self.base_url);
        let host_port = url_no_scheme.split('/').next().unwrap_or("localhost:80");
        let addr = if host_port.contains(':') {
            host_port.to_string()
        } else {
            format!("{}:80", host_port)
        };

        let mut stream = TcpStream::connect(&addr).map_err(|e| TungoError::local(&e.to_string()))?;
        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));

        let mut req = format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nAccept: {}\r\nConnection: close\r\nUser-Agent: tungo-sdk-rust/0.1.0\r\n",
            spec.method, full_path, host_port, accept
        );
        if spec.auth == "key" {
            let key = self
                .api_key
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| TungoError {
                    kind: TungoErrorKind::Auth,
                    status: 401,
                    code: "missing_api_key".to_string(),
                    message: "API ключ не задан".to_string(),
                    hint: None,
                    request_id: None,
                    retry_after: None,
                })?;
            req.push_str(&format!("x-api-key: {}\r\nAuthorization: Bearer {}\r\n", key, key));
        }
        if let Some(ik) = idem_key {
            req.push_str(&format!("Idempotency-Key: {}\r\n", ik));
        }
        for (k, v) in &spec.headers {
            req.push_str(&format!("{}: {}\r\n", k, v));
        }
        if let Some(b) = body_str {
            req.push_str("Content-Type: application/json\r\n");
            req.push_str(&format!("Content-Length: {}\r\n\r\n{}", b.len(), b));
        } else {
            req.push_str("Content-Length: 0\r\n\r\n");
        }

        stream
            .write_all(req.as_bytes())
            .map_err(|e| TungoError::local(&e.to_string()))?;

        let mut reader = BufReader::new(stream);
        self.read_http_response(&mut reader)
    }

    fn read_http_response<R: BufRead>(&self, reader: &mut R) -> Result<String, TungoError> {
        let mut status_line = String::new();
        reader
            .read_line(&mut status_line)
            .map_err(|e| TungoError::local(&e.to_string()))?;
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);

        let mut resp_headers = BTreeMap::new();
        loop {
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .map_err(|e| TungoError::local(&e.to_string()))?;
            if line == "\r\n" || line == "\n" || line.is_empty() {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                resp_headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }

        if let (Some(lim), Some(rem), Some(res)) = (
            resp_headers.get("x-ratelimit-limit").and_then(|s| s.parse().ok()),
            resp_headers.get("x-ratelimit-remaining").and_then(|s| s.parse().ok()),
            resp_headers.get("x-ratelimit-reset").and_then(|s| s.parse().ok()),
        ) {
            if let Ok(mut g) = self.last_rate_limit.lock() {
                *g = Some(RateLimitInfo {
                    limit: lim,
                    remaining: rem,
                    reset: res,
                });
            }
        }

        let mut resp_body = String::new();
        reader
            .read_to_string(&mut resp_body)
            .map_err(|e| TungoError::local(&e.to_string()))?;

        if (200..300).contains(&status) {
            Ok(resp_body)
        } else {
            Err(parse_api_error(status, &resp_body, &resp_headers))
        }
    }

    /// Организации (workspace), доступные ключу API
    pub fn workspaces(&self) -> crate::resources::WorkspacesResource<'_> {
        crate::resources::WorkspacesResource { client: self }
    }

    /// Компании организации и их карточки
    pub fn companies(&self) -> crate::resources::CompaniesResource<'_> {
        crate::resources::CompaniesResource { client: self }
    }

    /// Баланс платформы, книга движений, отчёты о расходе, лимиты, аудит и тарифы моделей
    pub fn platform(&self) -> crate::resources::PlatformResource<'_> {
        crate::resources::PlatformResource { client: self }
    }

    /// Сведения о текущем ключе API и статистика его расхода
    pub fn api_keys(&self) -> crate::resources::ApiKeysResource<'_> {
        crate::resources::ApiKeysResource { client: self }
    }

    /// Исходящие вебхуки организации (Svix): адреса, каталог событий, доставки и проверка подписи
    pub fn webhooks(&self) -> crate::resources::WebhooksResource<'_> {
        crate::resources::WebhooksResource { client: self }
    }

    /// Диалоги с ИИ-бухгалтером: вопросы, потоковый чат (SSE), подсчёт токенов и история
    pub fn chat(&self) -> crate::resources::ChatResource<'_> {
        crate::resources::ChatResource { client: self }
    }

    /// Предложения документов ИИ-бухгалтера: просмотр, одобрение записи в 1С и отклонение
    pub fn proposals(&self) -> crate::resources::ProposalsResource<'_> {
        crate::resources::ProposalsResource { client: self }
    }

    /// Лента событий и предложений компании (страницы и живой поток SSE)
    pub fn feed(&self) -> crate::resources::FeedResource<'_> {
        crate::resources::FeedResource { client: self }
    }

    /// Пачки обработки банковских выписок, входящих ЭСФ и DirectBank
    pub fn batches(&self) -> crate::resources::BatchesResource<'_> {
        crate::resources::BatchesResource { client: self }
    }

    /// Входящие и исходящие электронные счета-фактуры (ИС ЭСФ)
    pub fn esf(&self) -> crate::resources::EsfResource<'_> {
        crate::resources::EsfResource { client: self }
    }

    /// Налоговые формы (ФНО 100.00 и др.): сборка, проверка ИСНА и разрывы данных
    pub fn forms(&self) -> crate::resources::FormsResource<'_> {
        crate::resources::FormsResource { client: self }
    }

    /// Состояние подключения компании к базе 1С
    pub fn onec(&self) -> crate::resources::OnecResource<'_> {
        crate::resources::OnecResource { client: self }
    }

    /// Автоматизации компании: входящие хуки, сценарии, цепочки шагов, запуски и уровень автономности ИИ
    pub fn automations(&self) -> crate::resources::AutomationsResource<'_> {
        crate::resources::AutomationsResource { client: self }
    }

}

fn url_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn gen_idempotency_key() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(1);
    format!("idem-{:x}", nanos)
}
