# Futures Order-Book Engine

Real-time **Binance USDⓈ-M Futures BTCUSDT perpetual** order-book and trade-ingestion engine in Rust.

> **This engine represents the Binance BTCUSDT USDⓈ-M Futures order book and executed trades. It must not be interpreted as the Binance Spot order book.**

---

## Overview

Phase 1 provides a local order book synchronized against Binance Futures depth updates.

Phase 2 adds real-time Futures executed-trade ingestion alongside the order book.

```
                  Binance USDⓈ-M Futures
                         │
             ┌───────────┴───────────┐
             │                       │
             ▼                       ▼
      btcusdt@depth@100ms       btcusdt@trade
             │                       │
             ▼                       ▼
       OrderBook Engine          Trade Engine
             │                       │
             └───────────┬───────────┘
                         ▼
                  MarketEvent Bus
                         │
                         ▼
                 Diagnostics / Future
                    Analytics
```

## Market Definition

| Field | Value |
|-------|-------|
| Exchange | Binance |
| Product | USDⓈ-M Futures |
| Contract | BTCUSDT perpetual |
| Symbol | BTCUSDT |
| Tick Size | 0.10 |
| Step Size | 0.001 BTC |

## Why USDⓈ-M Futures (Not Spot)

Our eventual order-flow analysis must compare Futures resting liquidity against Futures executed trades from the same market. Using Spot data would produce an inconsistent and misleading view of market microstructure.

---

## Phase 1 — Local Order Book

### REST Snapshot

```
GET https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000
```

### WebSocket Depth Stream

```
wss://fstream.binance.com/ws/btcusdt@depth@100ms
```

**Stream names are lowercase and case-sensitive.**

### Diff-Depth Message Structure

```json
{
  "e": "depthUpdate",
  "E": 1234567890000,
  "T": 1234567890000,
  "s": "BTCUSDT",
  "U": 100,
  "u": 105,
  "pu": 99,
  "b": [["50000.10", "1.5"]],
  "a": [["50000.20", "0.5"]]
}
```

### Synchronization Rules

1. Open WebSocket, buffer events.
2. Fetch REST snapshot (`lastUpdateId`).
3. Drop buffered events where `u < lastUpdateId`.
4. First processed event must satisfy: `U ≤ lastUpdateId AND u ≥ lastUpdateId`.
5. Each subsequent event must have: `event.pu == previous_event.u`.
6. A `pu` mismatch triggers immediate resynchronization.

### Exact Numeric Representation

All prices and quantities stored as `u64` integer ticks scaled by 1e8. No `f64` used as authoritative keys. This guarantees exact equality comparison.

---

## Phase 2 — Futures Trade Ingestion

### Trade Stream

```
wss://fstream.binance.com/ws/btcusdt@trade
```

> **Note:** Binance Futures uses `@trade` for individual trade events, not `@aggTrade` (which is a Spot-only stream name).

### Futures Trade Payload

```json
{
  "e": "trade",
  "E": 1787137583835,
  "T": 1787137583835,
  "s": "BTCUSDT",
  "t": 7978350772,
  "p": "64486.00",
  "q": "0.002",
  "X": "MARKET",
  "m": false,
  "st": 1
}
```

| Field | Meaning |
|-------|---------|
| `e` | Event type ("trade") |
| `E` | Event time (ms) |
| `T` | Trade time (ms) |
| `s` | Symbol |
| `t` | Trade ID |
| `p` | Price (string) |
| `q` | Quantity (string) |
| `X` | Order type |
| `m` | **Is buyer maker** |
| `st` | Trade type |

### Aggressor-Side Classification

The `m` (is buyer maker) field determines which side aggressed:

| Binance `m` | Buyer is maker? | Aggressor | AggressorSide |
|-------------|-----------------|-----------|---------------|
| `true`      | Yes             | Seller    | `Sell`        |
| `false`     | No              | Buyer     | `Buy`         |

The **aggressor** is the party that submitted a marketable order consuming resting liquidity.

An **aggressive BUY** means a buyer consumed resting asks.

An **aggressive SELL** means a seller consumed resting bids.

This classification is critical for future CVD, delta, absorption, and sweep analysis.

### Duplicate Detection

A bounded window of 4096 recent trade IDs is maintained. If a trade ID is received twice within this window, it is flagged as a duplicate and not processed again. The window uses FIFO eviction to maintain a fixed memory footprint.

### Trade and Order-Book Independence

Trade events do NOT modify the order book. Order-book events do NOT modify trade state. Each stream has independent reconnection, error handling, and metrics. A trade-stream disconnection does not trigger an order-book resynchronization.

---

## Running

### Prerequisites

- Rust 1.70+ (with `cargo`)

### Build

```bash
cd futures-orderbook
cargo build --release
```

### Run (Live)

```bash
cargo run --release -- --symbol BTCUSDT --duration 60
```

### Run (With Debug Logging)

```bash
RUST_LOG=debug cargo run --release -- --symbol BTCUSDT --duration 60
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--symbol` | BTCUSDT | Trading symbol |
| `--duration` | 0 (indefinite) | Run duration in seconds |
| `--depth-speed` | 100ms | Depth update speed |
| `--depth-limit` | 1000 | REST snapshot depth levels |
| `--diagnostic-interval` | 2 | Diagnostic print interval (seconds) |
| `--reconnect-base-ms` | 1000 | Reconnect base delay |
| `--reconnect-max-ms` | 30000 | Reconnect max delay |

---

## Testing

### Unit Tests + Integration Tests

```bash
cargo test
```

### Format Check

```bash
cargo fmt --check
```

### Lint

```bash
cargo clippy
```

### Live Integration Test

```bash
cargo run --release -- --symbol BTCUSDT --duration 60
```

The live test is successful if:
- Both depth and trade WebSockets connect (HTTP 101)
- REST snapshot is received
- Order book reaches READY state
- Best Bid and Best Ask are populated
- Trades are received and processed
- Buy/sell aggressor counts are non-zero
- No unexplained resyncs or sequence errors

---

## Diagnostics Output

```
BTCUSDT PERPETUAL
Market: Binance USDⓈ-M Futures

Order Book
Status: Ready
Best Bid:  65432.00
Best Ask:  65432.10
Mid:       65432.05
Spread:    0.10

Trades
Status: CONNECTED
Trades Received: 2308
Trades Processed: 2308
Duplicates: 0
Buy Aggressors: 930
Sell Aggressors: 1378

Last Trade:
  Price:     65432.10
  Quantity:  0.0030
  Aggressor: BUY
  Trade ID:  7979165541

Uptime: 54s
Buffer size: 0
```

---

## Architecture

```
src/
├── main.rs                    # Engine loop + CLI
├── config.rs                  # CLI args + URL builders
├── error.rs                   # Error types
├── binance/
│   ├── mod.rs
│   ├── types.rs               # DepthUpdate, DepthSnapshot (serde)
│   ├── trade_types.rs         # FuturesTrade (aggTrade serde)
│   ├── rest.rs                # REST client (/fapi/v1/depth)
│   ├── websocket.rs           # Depth WebSocket client
│   └── ws_trades.rs           # Trade WebSocket client
├── orderbook/
│   ├── mod.rs
│   ├── book.rs                # Core OrderBook (BTreeMap)
│   ├── level.rs               # Integer tick representation
│   └── synchronizer.rs        # State machine + reconciliation
├── trades/
│   ├── mod.rs
│   ├── trade.rs               # TradeEvent + AggressorSide
│   ├── normalizer.rs           # FuturesTrade → TradeEvent
│   └── processor.rs            # Duplicate detection + metrics
├── events/
│   ├── mod.rs
│   └── market.rs              # MarketEvent enum
└── diagnostics/
    └── mod.rs                 # CLI display + metrics
```

---

## Known Limitations

- Phase 2 only implements the `btcusdt@trade` stream. The `@aggTrade` stream name does not exist on Binance Futures (it is Spot-only).
- No database, frontend, or trading functionality.
- No CVD, delta, absorption, or sweep analytics (deferred to later phases).
- Price display uses 2 decimal places — very small tick values may display as 0.00 in diagnostics.
- The 4096-entry duplicate window may miss duplicates that arrive after the window has evicted the original trade ID.

---

## License

Internal project — not yet licensed for distribution.
