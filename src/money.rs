//! Money helpers: exact conversion between tenge and tiyn, formatting, and
//! BIN/IIN validation.
//!
//! The API expresses every amount in tiyn (1/100 of a tenge) as a 64-bit
//! integer. Convert decimal strings with [`to_tiyn`]: it never goes through
//! binary floating point.

/// A money amount could not be converted to tiyn.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MoneyError {
    /// The text is not a plain decimal number (`-?digits[.digits]`).
    #[error(
        "`{0}` is not a decimal amount (expected digits with an optional fraction, e.g. 1234.56)"
    )]
    Invalid(String),
    /// The amount does not fit into a 64-bit number of tiyn.
    #[error("the amount `{0}` is out of range")]
    OutOfRange(String),
    /// The float is NaN or infinite.
    #[error("the amount is not a finite number")]
    NotFinite,
}

/// Converts a decimal amount in tenge to tiyn, exactly.
///
/// The input must match `-?digits[.digits]` (ASCII only, no spaces, no
/// exponent). The result is rounded half away from zero to two places.
///
/// ```
/// use tungo_sdk::money::to_tiyn;
///
/// assert_eq!(to_tiyn("45000"), Ok(4_500_000));
/// assert_eq!(to_tiyn("1.005"), Ok(101));
/// assert_eq!(to_tiyn("-1.005"), Ok(-101));
/// assert!(to_tiyn("1,5").is_err());
/// ```
pub fn to_tiyn(amount: &str) -> Result<i64, MoneyError> {
    let invalid = || MoneyError::Invalid(amount.to_owned());
    let out_of_range = || MoneyError::OutOfRange(amount.to_owned());
    let (negative, unsigned) = match amount.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, amount),
    };
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (unsigned, ""),
    };
    let all_digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if whole.is_empty() || !all_digits(whole) || !all_digits(fraction) {
        return Err(invalid());
    }
    if unsigned.contains('.') && fraction.is_empty() {
        return Err(invalid());
    }
    let mut tiyn: i128 = 0;
    for digit in whole.bytes() {
        tiyn = tiyn
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(digit - b'0')))
            .ok_or_else(out_of_range)?;
    }
    let fraction = fraction.as_bytes();
    let digit_at = |index: usize| i128::from(fraction.get(index).map_or(0, |d| d - b'0'));
    tiyn = tiyn
        .checked_mul(100)
        .and_then(|value| value.checked_add(digit_at(0) * 10 + digit_at(1)))
        .ok_or_else(out_of_range)?;
    if digit_at(2) >= 5 {
        tiyn += 1;
    }
    if negative {
        tiyn = -tiyn;
    }
    i64::try_from(tiyn).map_err(|_| out_of_range())
}

/// Converts a binary float amount in tenge to tiyn.
///
/// The float is converted through its shortest decimal representation that
/// round-trips, so `1.005` gives `101`. Prefer [`to_tiyn`] with a string:
/// a float may already have lost precision before it reaches this function.
///
/// ```
/// use tungo_sdk::money::to_tiyn_f64;
///
/// assert_eq!(to_tiyn_f64(1.005), Ok(101));
/// assert!(to_tiyn_f64(f64::NAN).is_err());
/// ```
pub fn to_tiyn_f64(amount: f64) -> Result<i64, MoneyError> {
    if !amount.is_finite() {
        return Err(MoneyError::NotFinite);
    }
    // `Display` for `f64` prints the shortest round-trip decimal without an exponent.
    to_tiyn(&amount.to_string())
}

/// Converts a `rust_decimal::Decimal` amount in tenge to tiyn, exactly.
#[cfg(feature = "rust_decimal")]
#[cfg_attr(docsrs, doc(cfg(feature = "rust_decimal")))]
pub fn to_tiyn_decimal(amount: rust_decimal::Decimal) -> Result<i64, MoneyError> {
    to_tiyn(&amount.to_string())
}

/// Converts tiyn to a `rust_decimal::Decimal` amount in tenge, exactly.
#[cfg(feature = "rust_decimal")]
#[cfg_attr(docsrs, doc(cfg(feature = "rust_decimal")))]
pub fn to_decimal(tiyn: i64) -> rust_decimal::Decimal {
    rust_decimal::Decimal::new(tiyn, 2)
}

/// Formats tiyn as tenge: `4500000` becomes `45 000,00 ₸`.
///
/// ```
/// use tungo_sdk::money::format_tiyn;
///
/// assert_eq!(format_tiyn(4_500_000), "45 000,00 ₸");
/// assert_eq!(format_tiyn(-50), "-0,50 ₸");
/// ```
pub fn format_tiyn(tiyn: i64) -> String {
    format_tiyn_in(tiyn, "KZT")
}

/// Formats minor units in the given currency. `KZT`, `USD`, `EUR` and `RUB`
/// (in any case) use their symbol; any other code is printed upper-cased:
/// `1 234,56 GBP`.
pub fn format_tiyn_in(tiyn: i64, currency: &str) -> String {
    let code = currency.to_ascii_uppercase();
    let symbol = match code.as_str() {
        "" | "KZT" => "₸",
        "USD" => "$",
        "EUR" => "€",
        "RUB" => "₽",
        other => other,
    };
    let magnitude = tiyn.unsigned_abs();
    let whole = (magnitude / 100).to_string();
    let mut grouped = String::with_capacity(whole.len() + whole.len() / 3);
    for (index, digit) in whole.chars().enumerate() {
        if index > 0 && (whole.len() - index) % 3 == 0 {
            grouped.push(' ');
        }
        grouped.push(digit);
    }
    let sign = if tiyn < 0 { "-" } else { "" };
    format!("{sign}{grouped},{:02} {symbol}", magnitude % 100)
}

/// Checks a Kazakhstan BIN or IIN: exactly 12 ASCII digits with a correct
/// check digit. The input is not trimmed.
///
/// ```
/// use tungo_sdk::money::is_valid_bin_iin;
///
/// assert!(is_valid_bin_iin("990140000017"));
/// assert!(!is_valid_bin_iin("990140000016"));
/// assert!(!is_valid_bin_iin(" 990140000017"));
/// ```
pub fn is_valid_bin_iin(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 12 || !bytes.iter().all(u8::is_ascii_digit) {
        return false;
    }
    let digits: Vec<u32> = bytes.iter().map(|b| u32::from(b - b'0')).collect();
    let weighted = |weights: [u32; 11]| -> u32 {
        digits.iter().zip(weights).map(|(d, w)| d * w).sum::<u32>() % 11
    };
    let mut control = weighted([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    if control == 10 {
        control = weighted([3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2]);
    }
    control < 10 && control == digits[11]
}

/// The money helpers under the name the other Tungo SDKs use.
///
/// ```
/// use tungo_sdk::TungoMoney;
///
/// assert_eq!(TungoMoney::to_tiyn("120.00"), Ok(12_000));
/// assert_eq!(TungoMoney::format_tiyn(12_000), "120,00 ₸");
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TungoMoney;

impl TungoMoney {
    /// See [`to_tiyn`].
    pub fn to_tiyn(amount: &str) -> Result<i64, MoneyError> {
        to_tiyn(amount)
    }

    /// See [`to_tiyn_f64`].
    pub fn to_tiyn_f64(amount: f64) -> Result<i64, MoneyError> {
        to_tiyn_f64(amount)
    }

    /// See [`format_tiyn`].
    pub fn format_tiyn(tiyn: i64) -> String {
        format_tiyn(tiyn)
    }

    /// See [`format_tiyn_in`].
    pub fn format_tiyn_in(tiyn: i64, currency: &str) -> String {
        format_tiyn_in(tiyn, currency)
    }

    /// See [`is_valid_bin_iin`].
    pub fn is_valid_bin_iin(value: &str) -> bool {
        is_valid_bin_iin(value)
    }
}
