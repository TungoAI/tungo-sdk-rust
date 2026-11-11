use tungo_sdk::money::{format_tiyn, format_tiyn_in, is_valid_bin_iin, to_tiyn, to_tiyn_f64};

fn main() -> Result<(), tungo_sdk::MoneyError> {
    // Decimal strings are converted exactly, rounding half away from zero.
    let tiyn = to_tiyn("45000.50")?;
    assert_eq!(tiyn, 4_500_050);
    assert_eq!(to_tiyn("1.005")?, 101);
    assert!(to_tiyn("1,5").is_err());

    // A float goes through its shortest decimal form; prefer strings.
    assert_eq!(to_tiyn_f64(1.005)?, 101);
    assert!(to_tiyn_f64(f64::NAN).is_err());

    assert_eq!(format_tiyn(tiyn), "45 000,50 ₸");
    assert_eq!(format_tiyn_in(123_456, "USD"), "1 234,56 $");
    assert!(is_valid_bin_iin("990140000017"));
    println!("{}", format_tiyn(tiyn));
    Ok(())
}
