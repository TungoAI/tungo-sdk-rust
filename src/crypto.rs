//! SHA-256, HMAC-SHA256, Base64, подпись входящих хуков (/hooks/{token}) и проверка Svix-вебхуков.

use crate::client::{TungoError, WebhookEnvelope};
use crate::json::JsonValue;
use std::collections::BTreeMap;

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    let bit_len = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut k_block = [0u8; 64];
    if key.len() > 64 {
        let hashed = sha256(key);
        k_block[..32].copy_from_slice(&hashed);
    } else {
        k_block[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k_block[i];
        opad[i] ^= k_block[i];
    }
    let mut inner = Vec::with_capacity(64 + message.len());
    inner.extend_from_slice(&ipad);
    inner.extend_from_slice(message);
    let inner_hash = sha256(&inner);

    let mut outer = Vec::with_capacity(64 + 32);
    outer.extend_from_slice(&opad);
    outer.extend_from_slice(&inner_hash);
    sha256(&outer)
}

pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

const B64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_CHARS[((triple >> 18) & 0x3f) as usize] as char);
        out.push(B64_CHARS[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_CHARS[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64_CHARS[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut vals = Vec::with_capacity(input.len());
    for b in input.bytes() {
        let v = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            b' ' | b'\n' | b'\r' | b'\t' => continue,
            _ => return Err("invalid base64 character".to_string()),
        };
        vals.push(v);
    }
    let mut out = Vec::with_capacity(vals.len() * 3 / 4);
    for chunk in vals.chunks(4) {
        if chunk.len() < 2 {
            break;
        }
        out.push((chunk[0] << 2) | (chunk[1] >> 4));
        if chunk.len() > 2 {
            out.push((chunk[1] << 4) | (chunk[2] >> 2));
        }
        if chunk.len() > 3 {
            out.push((chunk[2] << 6) | chunk[3]);
        }
    }
    Ok(out)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Вычисляет подпись X-Signature (sha256=<hex>) для входящего хука /hooks/{token}.
pub fn sign_hook_payload(raw_body: &str, secret: &str) -> String {
    let mac = hmac_sha256(secret.as_bytes(), raw_body.as_bytes());
    format!("sha256={}", hex_encode(&mac))
}

/// Проверяет подпись входящего хука (sha256=<hex>).
pub fn verify_hook_signature(raw_body: &str, signature: &str, secret: &str) -> bool {
    let expected = sign_hook_payload(raw_body, secret);
    constant_time_eq(expected.as_bytes(), signature.trim().as_bytes())
}

/// Проверяет Svix-подпись исходящего вебхука организации (whsec_...) и разбирает конверт события.
pub fn verify_webhook(
    raw_body: &str,
    headers: &BTreeMap<String, String>,
    secret: &str,
    now_unix: Option<i64>,
    tolerance_sec: i64,
) -> Result<WebhookEnvelope, TungoError> {
    let find_h = |names: &[&str]| -> Option<String> {
        for (k, v) in headers {
            let kl = k.to_ascii_lowercase();
            if names.iter().any(|n| *n == kl) {
                return Some(v.clone());
            }
        }
        None
    };

    let msg_id = find_h(&["svix-id", "webhook-id"]).ok_or_else(|| {
        TungoError::local("отсутствует заголовок svix-id")
    })?;
    let msg_ts = find_h(&["svix-timestamp", "webhook-timestamp"]).ok_or_else(|| {
        TungoError::local("отсутствует заголовок svix-timestamp")
    })?;
    let msg_sig = find_h(&["svix-signature", "webhook-signature"]).ok_or_else(|| {
        TungoError::local("отсутствует заголовок svix-signature")
    })?;

    let ts: i64 = msg_ts
        .parse()
        .map_err(|_| TungoError::local("некорректный svix-timestamp"))?;
    if let Some(now) = now_unix {
        if tolerance_sec > 0 && (now - ts).abs() > tolerance_sec {
            return Err(TungoError::local("метка времени вебхука вне допустимого окна"));
        }
    }

    let clean_secret = secret.strip_prefix("whsec_").unwrap_or(secret);
    let key_bytes = base64_decode(clean_secret)
        .map_err(|_| TungoError::local("некорректный секрет whsec_"))?;

    let signed = format!("{}.{}.{}", msg_id, msg_ts, raw_body);
    let mac = hmac_sha256(&key_bytes, signed.as_bytes());
    let expected_b64 = base64_encode(&mac);

    let matched = msg_sig.split_whitespace().any(|part| {
        if let Some(cand) = part.strip_prefix("v1,") {
            constant_time_eq(expected_b64.as_bytes(), cand.as_bytes())
        } else {
            false
        }
    });
    if !matched {
        return Err(TungoError::local("неверная подпись svix-signature"));
    }

    let parsed = JsonValue::parse(raw_body).map_err(|e| TungoError::local(&e))?;
    let ev = parsed
        .get("type")
        .or_else(|| parsed.get("event"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let ts_str = parsed
        .get("created_at")
        .or_else(|| parsed.get("timestamp"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    Ok(WebhookEnvelope {
        event: ev,
        timestamp: ts_str,
        company_id: parsed.get("company_id").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
        data: parsed.get("data").cloned().unwrap_or(JsonValue::Null),
    })
}
