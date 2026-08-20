//! Reads recorded market data back from storage and merges it into a single
//! deterministic event stream ordered by the recorder's global sequence.

use uuid::Uuid;

use crate::binance::types::DepthUpdate;
use crate::orderbook::level::{ticks_to_price_str, ticks_to_quantity_str};
use crate::storage::{
    datetime_to_ms, datetime_to_ns, LevelChangeRow, RawEventRow, Storage, TradeRow,
};
use crate::trades::trade::{AggressorSide, TradeEvent};

/// A single replayable event, reconstructable from the normalized rows.
#[derive(Debug, Clone)]
pub enum ReplayEvent {
    /// An order-book snapshot (initial sync or resync).
    Snapshot {
        seq: u64,
        update_id: u64,
        time_ms: u64,
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
    },
    /// A depth update, reconstructed from its per-level rows.
    Depth { seq: u64, update: DepthUpdate },
    /// A normalized trade.
    Trade { seq: u64, event: TradeEvent },
}

impl ReplayEvent {
    /// The recorder sequence that determines replay ordering.
    pub fn seq(&self) -> u64 {
        match self {
            ReplayEvent::Snapshot { seq, .. }
            | ReplayEvent::Depth { seq, .. }
            | ReplayEvent::Trade { seq, .. } => *seq,
        }
    }

    /// Exchange event time (ms) used for real-time pacing.
    pub fn time_ms(&self) -> u64 {
        match self {
            ReplayEvent::Snapshot { time_ms, .. } => *time_ms,
            ReplayEvent::Depth { update, .. } => update.event_time,
            ReplayEvent::Trade { event, .. } => event.trade_time,
        }
    }
}

/// All events for a session, merged and sorted by `seq`.
pub struct SessionData {
    pub events: Vec<ReplayEvent>,
}

impl SessionData {
    pub fn new(mut events: Vec<ReplayEvent>) -> Self {
        events.sort_by_key(ReplayEvent::seq);
        Self { events }
    }
}

/// Load all recorded data for a session and merge into one ordered stream.
pub async fn load_session(storage: &dyn Storage, session_id: Uuid) -> anyhow::Result<SessionData> {
    let snapshots = storage.read_snapshots(session_id).await?;
    let level_changes = storage.read_level_changes(session_id).await?;
    let trades = storage.read_trades(session_id).await?;

    let mut events: Vec<ReplayEvent> =
        Vec::with_capacity(snapshots.len() + trades.len() + level_changes.len());

    for s in snapshots {
        events.push(ReplayEvent::Snapshot {
            seq: s.seq,
            update_id: s.snapshot_update_id,
            time_ms: datetime_to_ms(s.timestamp),
            bids: decode_levels(&s.bids)?,
            asks: decode_levels(&s.asks)?,
        });
    }

    for (seq, update) in reconstruct_depth_updates(&level_changes) {
        events.push(ReplayEvent::Depth { seq, update });
    }

    for t in trades {
        events.push(ReplayEvent::Trade {
            seq: t.seq,
            event: trade_row_to_event(&t),
        });
    }

    Ok(SessionData::new(events))
}

/// Load all raw events for a session (used by the verify module).
pub async fn load_raw_events(
    storage: &dyn Storage,
    session_id: Uuid,
) -> anyhow::Result<Vec<RawEventRow>> {
    storage.read_raw_events(session_id).await
}

/// Decode a JSON-encoded `[[price_ticks, qty_ticks], ...]` level set back to
/// the string pairs consumed by the order book.
pub fn decode_levels(json: &str) -> anyhow::Result<Vec<(String, String)>> {
    let pairs: Vec<(u64, u64)> = serde_json::from_str(json)?;
    Ok(pairs
        .into_iter()
        .map(|(p, q)| (ticks_to_price_str(p), ticks_to_quantity_str(q)))
        .collect())
}

/// Reconstruct `DepthUpdate` events from their per-level rows.
///
/// All rows of a single depth event share the same `seq` (assigned by the
/// recorder). Bids and asks are emitted in stored order; the order book only
/// cares about set semantics per price level, so ordering is irrelevant.
fn reconstruct_depth_updates(rows: &[LevelChangeRow]) -> Vec<(u64, DepthUpdate)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        let seq = rows[i].seq;
        let mut bids: Vec<(String, String)> = Vec::new();
        let mut asks: Vec<(String, String)> = Vec::new();

        let symbol = rows[i].symbol.clone();
        let event_time = datetime_to_ms(rows[i].exchange_event_time);
        let transaction_time = datetime_to_ms(rows[i].exchange_transaction_time);
        let first_update_id = rows[i].first_update_id;
        let final_update_id = rows[i].final_update_id;
        let previous_final_update_id = rows[i].previous_final_update_id;

        while i < rows.len() && rows[i].seq == seq {
            let r = &rows[i];
            let pair = (
                ticks_to_price_str(r.price),
                ticks_to_quantity_str(r.quantity),
            );
            if r.side == "BID" {
                bids.push(pair);
            } else {
                asks.push(pair);
            }
            i += 1;
        }

        out.push((
            seq,
            DepthUpdate {
                event_type: "depthUpdate".to_string(),
                event_time,
                transaction_time,
                symbol,
                first_update_id,
                final_update_id,
                previous_final_update_id,
                bids,
                asks,
            },
        ));
    }
    out
}

/// Convert a stored trade row back to a `TradeEvent`.
pub fn trade_row_to_event(r: &TradeRow) -> TradeEvent {
    TradeEvent {
        symbol: r.symbol.clone(),
        trade_id: r.trade_id,
        price_ticks: r.price,
        quantity_ticks: r.quantity,
        event_time: datetime_to_ms(r.exchange_event_time),
        trade_time: datetime_to_ms(r.trade_time),
        local_receive_time_ns: datetime_to_ns(r.local_receive_time),
        aggressor: if r.aggressor_side == "BUY" {
            AggressorSide::Buy
        } else {
            AggressorSide::Sell
        },
        order_type: r.order_type.clone(),
    }
}
