# Binance USDⓈ-M Futures BTCUSDT Order-Book Engine

> **This Phase 1 engine represents the Binance BTCUSDT USDⓈ-M Futures order book. It must not be interpreted as the Binance Spot order book.**

A real-time local order-book engine for the Binance BTCUSDT USDⓈ-M perpetual futures contract. This is Phase 1 of a future Bookmap-style order-flow platform.

## Why USDⓈ-M Futures (Not Spot)

This engine exclusively uses Binance's **USDⓈ-M Futures** APIs. It does **not** use:

- Binance Spot APIs
- Binance COIN-M Futures APIs
- Any spot WebSocket streams or REST endpoints

The target market is the **BTCUSDT perpetual contract** on Binance USDⓈ-M Futures, which is the contract we will eventually trade.

## BTCUSDT Perpetual Market Definition

- **Exchange:** Binance
- **Market type:** USDⓈ-M Futures
- **Symbol:** BTCUSDT
- **Contract type:** Perpetual
- **Margin asset:** USDT
- **Base asset:** BTC

## Binance Futures REST Endpoint

```
GET https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000
```

Valid depth limits: `5, 10, 20, 50, 100, 500, 1000`

Response fields:
- `lastUpdateId` — last update ID in the snapshot
- `bids` — array of `[price_string, quantity_string]`
- `asks` — array of `[price_string, quantity_string]`
- `T` — timestamp
- `E` — last update time

## Binance Futures WebSocket Endpoint

```
wss://fstream.binance.com/ws/btcusdt@depth@100ms
```

## Diff-Depth Message Structure

```json
{
  "e": "depthUpdate",
  "E": 123456789,
  "T": 123456788,
  "s": "BTCUSDT",
  "U": 150,
  "u": 160,
  "pu": 149,
  "b": [["50000.10", "1.500"], ...],
  "a": [["50000.20", "0.500"], ...]
}
```

Fields:
- `e` — event type (`depthUpdate`)
- `E` — event time (ms)
- `T` — transaction time (ms)
- `s` — symbol
- `U` — first update ID in this event
- `u` — final update ID in this event
- `pu` — final update ID of the previous stream event (Futures-specific)
- `b` — bid updates: `[price, quantity]` absolute values
- `a` — ask updates: `[price, quantity]` absolute values

Each bid/ask update is an **absolute quantity** for that price level. If quantity is zero, the level is removed.

## Local Order-Book Synchronization

The synchronization procedure follows Binance's official documentation:

1. **Open WebSocket** to `wss://fstream.binance.com/ws/btcusdt@depth@100ms`
2. **Buffer** incoming depth events while waiting for the snapshot.
3. **Fetch REST snapshot** from `GET /fapi/v1/depth?symbol=BTCUSDT&limit=1000`
4. **Discard** buffered events where `u < lastUpdateId` from the snapshot.
5. **Find the first bridging event** where `U <= lastUpdateId AND u >= lastUpdateId`.
6. **Initialize** the local book from the snapshot.
7. **Apply** the bridging event and all subsequent buffered events.
8. **Continue** applying live events, validating `pu` continuity.
9. **If continuity breaks:** invalidate the book, trigger resynchronization.

## `U`, `u`, and `pu` Semantics

- **`U`** (first_update_id): The first update ID covered by this event.
- **`u`** (final_update_id): The last update ID covered by this event.
- **`pu`** (previous_final_update_id): The `u` value of the immediately preceding event on the stream.

**Synchronization invariant:**

```
First bridging event: U <= lastUpdateId AND u >= lastUpdateId
Every subsequent event: event.pu == previous_event.u
```

The `pu` field is **Futures-specific** and is not available on Binance Spot.

## Resynchronization Behavior

When a sequence gap is detected (pu mismatch, missing events, or WebSocket reconnection):

1. Mark the local book as invalid.
2. Stop exposing it as authoritative.
3. Trigger resynchronization:
   - Clear the buffer.
   - Reconnect WebSocket if needed.
   - Buffer new depth events.
   - Fetch a fresh REST snapshot.
   - Reconcile buffered events.
   - Return to READY only after sequence continuity is established.

## Exact Price/Quantity Representation

Prices and quantities are stored as **integer ticks** scaled by 10^8:

```rust
pub const TICK_SCALE: u64 = 100_000_000; // 1e8
```

This guarantees **exact equality comparison** with no floating-point errors. A price of `50000.50` is stored as `5_000_050_000_000`.

## Running the Application

```bash
# Build
cargo build --release

# Run with default settings (BTCUSDT, run indefinitely)
cargo run --release

# Run with specific duration
cargo run --release -- --symbol BTCUSDT --duration 60

# Run with custom settings
cargo run --release -- \
    --symbol BTCUSDT \
    --depth-limit 1000 \
    --diagnostic-interval 2 \
    --duration 120
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--symbol` | `BTCUSDT` | Trading symbol |
| `--rest-base` | `https://fapi.binance.com` | REST API base URL |
| `--ws-base` | `wss://fstream.binance.com` | WebSocket base URL |
| `--depth-speed` | `100ms` | Depth update speed |
| `--depth-limit` | `1000` | REST snapshot depth limit |
| `--reconnect-base-ms` | `1000` | Reconnect base delay (ms) |
| `--reconnect-max-ms` | `30000` | Reconnect max delay (ms) |
| `--diagnostic-interval` | `2` | Diagnostic print interval (seconds) |
| `--duration` | `0` | Run duration (0 = indefinite) |

## Running Tests

```bash
# Run all tests
cargo test

# Run only unit tests
cargo test --lib

# Run only integration tests
cargo test --test integration_tests

# Run with output
cargo test -- --nocapture
```

## Running the Live Integration Test

```bash
cargo run --release -- --symbol BTCUSDT --duration 60
```

This will:
1. Connect to Binance USDⓈ-M Futures WebSocket
2. Subscribe to BTCUSDT depth stream
3. Fetch Futures REST snapshot
4. Synchronize local order book
5. Process live updates
6. Display diagnostics
7. Exit cleanly after the specified duration

## Diagnostic Output

```
BTCUSDT PERPETUAL
Market: Binance USDⓈ-M Futures

Status: Ready

Best Bid:  50000.10
Best Ask:  50000.20
Mid:       50000.15
Spread:    0.10

Last Update ID: 12345
Events Received: 5678
Events Applied:  5670
Events Ignored:  8
Resyncs: 0
Reconnects: 0
Sequence Errors: 0

Uptime: 60s
Buffer size: 0
```

## Known Limitations

- Phase 1 only implements depth stream; trades, aggTrades, bookTicker, mark price, funding rate, and open interest are not yet implemented.
- No database storage.
- No frontend or visualization.
- No trading functionality.
- No absorption, CVD, sweep, or liquidity-wall detection.
- Uses `f64` internally for string-to-tick conversion (immediately converted to exact integer ticks).
- The engine does not handle Binance rate limits beyond basic reconnection backoff.

## Architecture

```
futures_orderbook
├── src/
│   ├── main.rs              # CLI entry point and engine loop
│   ├── lib.rs               # Library root (exposes modules for testing)
│   ├── config.rs            # Configuration and CLI arguments
│   ├── error.rs             # Error types
│   ├── binance/
│   │   ├── mod.rs
│   │   ├── rest.rs          # REST client for snapshots
│   │   ├── websocket.rs     # WebSocket client with reconnection
│   │   └── types.rs         # Binance API types (serde)
│   ├── orderbook/
│   │   ├── mod.rs
│   │   ├── book.rs          # Core order book (BTreeMap-based)
│   │   ├── level.rs         # Price levels with integer tick representation
│   │   └── synchronizer.rs  # State machine and event reconciliation
│   ├── events/
│   │   ├── mod.rs
│   │   └── market.rs        # MarketEvent enum for future phases
│   └── diagnostics/
│       └── mod.rs           # Metrics and diagnostic display
└── tests/
    ├── fixtures/             # JSON test fixtures (Futures payloads)
    └── integration_tests.rs  # 39 comprehensive tests
```

## Test Coverage

- **39 tests** covering:
  - Basic book operations (empty book, snapshot, insert, update, remove)
  - Sequence handling (valid events, stale, duplicate, gap, pu mismatch)
  - Precision (exact ticks, no floating-point errors, deterministic)
  - Invariants (best bid/ask, no zero-quantity, no duplicates)
  - Synchronization (full flow, resync, buffered event reconciliation)
  - JSON deserialization (DepthUpdate, DepthSnapshot)
  - Edge cases (removal of nonexistent levels, snapshot replacement)
