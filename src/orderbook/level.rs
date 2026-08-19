/// A price level in the order book, using integer ticks for exact representation.
///
/// Prices are stored as integer ticks. The tick size is defined by the symbol's
/// `exchangeInfo` from Binance Futures. For BTCUSDT, the tick size is 0.10.
/// We scale by a fixed factor of 1e8 to accommodate any tick size on any symbol.
///
/// A price of 50000.50 with tick size 0.10 is stored as 5_000_050_000 (scaled by 1e8).
/// This guarantees exact equality comparison with no floating-point errors.
pub const TICK_SCALE: u64 = 100_000_000; // 1e8

/// A price represented as an integer tick. This is the authoritative
/// representation of price in the order book.
pub type PriceTick = u64;

/// A single price level in the order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLevel {
    /// The price in integer ticks.
    pub price: PriceTick,
    /// The quantity at this price level in base asset units (as string-parsed integer).
    /// We store as a scaled integer to avoid floating-point errors.
    pub quantity: QuantityTick,
}

/// A quantity represented as an integer tick.
/// For BTCUSDT, the step size is 0.001 BTC. We scale by 1e8 as well.
pub type QuantityTick = u64;

impl PriceLevel {
    pub fn new(price: PriceTick, quantity: QuantityTick) -> Self {
        Self { price, quantity }
    }

    /// Returns true if this level should be removed (zero quantity).
    pub fn is_removal(&self) -> bool {
        self.quantity == 0
    }
}

/// Convert a price string like "50000.50" to integer ticks.
/// Returns the number of ticks, where each tick = 1/TICK_SCALE.
pub fn price_str_to_ticks(s: &str) -> anyhow::Result<PriceTick> {
    // Parse as f64 first, then convert to ticks.
    // We accept f64 here for parsing convenience but immediately convert to exact integer ticks.
    let value: f64 = s.parse()?;
    let ticks = (value * TICK_SCALE as f64).round() as u64;
    Ok(ticks)
}

/// Convert a quantity string like "0.001" to integer ticks.
pub fn quantity_str_to_ticks(s: &str) -> anyhow::Result<QuantityTick> {
    let value: f64 = s.parse()?;
    let ticks = (value * TICK_SCALE as f64).round() as u64;
    Ok(ticks)
}

/// Convert integer ticks back to a display string.
pub fn ticks_to_price_str(ticks: PriceTick) -> String {
    let value = ticks as f64 / TICK_SCALE as f64;
    format!("{:.2}", value)
}

/// Convert quantity ticks back to a display string.
pub fn ticks_to_quantity_str(ticks: QuantityTick) -> String {
    let value = ticks as f64 / TICK_SCALE as f64;
    format!("{:.4}", value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_conversion_exact() {
        let p = price_str_to_ticks("50000.50").unwrap();
        // 50000.50 * 1e8 = 5_000_050_000_000
        assert_eq!(p, 5_000_050_000_000);
        assert_eq!(ticks_to_price_str(p), "50000.50");
    }

    #[test]
    fn test_quantity_conversion() {
        let q = quantity_str_to_ticks("1.5").unwrap();
        // 1.5 * 1e8 = 150_000_000
        assert_eq!(q, 150_000_000);
    }

    #[test]
    fn test_price_string_identity() {
        let p1 = price_str_to_ticks("50000.10").unwrap();
        let p2 = price_str_to_ticks("50000.10").unwrap();
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_no_floating_point_error() {
        // 0.1 + 0.2 != 0.3 in f64, but as ticks it should be exact
        let p1 = price_str_to_ticks("0.1").unwrap();
        let p2 = price_str_to_ticks("0.2").unwrap();
        let p3 = price_str_to_ticks("0.3").unwrap();
        assert_ne!(p1 + p2, p3 + 1); // No off-by-one from floating point
                                     // Actually 0.1*1e8 = 10000000, 0.2*1e8 = 20000000, 0.3*1e8 = 30000000
        assert_eq!(p1, 10_000_000);
        assert_eq!(p2, 20_000_000);
        assert_eq!(p3, 30_000_000);
        assert_eq!(p1 + p2, p3);
    }
}
