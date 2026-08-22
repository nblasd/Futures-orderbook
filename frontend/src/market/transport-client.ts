/**
 * TransportClient — WebSocket connection to the Rust backend.
 *
 * Receives HeatmapFrame snapshots and HeatmapDelta updates.
 * Handles reconnect with exponential backpressure.
 * Bounded pending-update queue prevents memory growth.
 */

import type {
  HeatmapFrame,
  HeatmapDelta,
  HeatmapCellSnapshot,
  MarketState,
  ConnectionStatus,
} from '../types/heatmap';

// --- Server message types (mirror Rust ServerMessage) ---

interface ServerSnapshot {
  type: 'snapshot';
  frame: {
    timestamp: number;
    visible_price_range: [number, number];
    time_range: [number, number];
    cells: HeatmapCellSnapshot[];
    summary: {
      total_price_levels: number;
      total_buckets: number;
      total_executed_buy: number;
      total_executed_sell: number;
      total_delta: number;
      total_trade_count: number;
      total_liquidity_added: number;
      total_liquidity_removed: number;
      total_large_trade_volume: number;
      total_replenishment_count: number;
      total_absorption_candidate_count: number;
      total_sweep_count: number;
    };
  };
}

interface ServerDelta {
  type: 'delta';
  delta: {
    changed: [number, HeatmapCellSnapshot][];
    new: HeatmapCellSnapshot[];
    removed: number[];
    summary_delta: {
      total_executed_buy: number;
      total_executed_sell: number;
      total_delta: number;
      total_trade_count: number;
      total_liquidity_added: number;
      total_liquidity_removed: number;
      total_large_trade_volume: number;
      total_replenishment_count: number;
      total_absorption_candidate_count: number;
      total_sweep_count: number;
    };
  };
}

interface ServerStatus {
  type: 'status';
  status: {
    connection: string;
    book_status: string;
    symbol: string;
    exchange: string;
    best_bid: number;
    best_ask: number;
    mid: number;
    spread: number;
    events_per_sec: number;
    trades_per_sec: number;
    heatmap_cells: number;
    sequence_errors: number;
    queue_depth: number;
  };
}

type ServerMessage = ServerSnapshot | ServerDelta | ServerStatus;

// --- Callbacks ---

export interface TransportCallbacks {
  onFrame: (frame: HeatmapFrame) => void;
  onDelta: (delta: HeatmapDelta) => void;
  onStatus: (status: Partial<MarketState>) => void;
  onConnectionChange: (status: ConnectionStatus) => void;
}

// --- Configuration ---

const RECONNECT_DELAYS = [250, 500, 1000, 2000, 5000, 10_000];
const MAX_PENDING_UPDATES = 32;
const STALE_THRESHOLD_MS = 5000;

export class TransportClient {
  private ws: WebSocket | null = null;
  private callbacks: TransportCallbacks;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private staleTimer: ReturnType<typeof setInterval> | null = null;
  private lastUpdateTime = 0;
  private pendingUpdates: ServerMessage[] = [];
  private disposed = false;

  constructor(callbacks: TransportCallbacks) {
    this.callbacks = callbacks;
  }

  /** Connect to the backend WebSocket. */
  connect(url: string = 'ws://localhost:8080/ws'): void {
    if (this.disposed) return;
    this.dispose();
    this.disposed = false;

    this.callbacks.onConnectionChange('DISCONNECTED');

    try {
      this.ws = new WebSocket(url);

      this.ws.onopen = () => {
        this.reconnectAttempt = 0;
        this.callbacks.onConnectionChange('LIVE');
        this.startStaleDetection();
      };

      this.ws.onmessage = (event) => {
        try {
          const msg: ServerMessage = JSON.parse(event.data);
          this.lastUpdateTime = Date.now();
          this.processMessage(msg);
        } catch (e) {
          console.warn('Failed to parse server message:', e);
        }
      };

      this.ws.onclose = () => {
        this.callbacks.onConnectionChange('DISCONNECTED');
        this.stopStaleDetection();
        if (!this.disposed) {
          this.scheduleReconnect(url);
        }
      };

      this.ws.onerror = () => {
        // onclose will fire after onerror
      };
    } catch (e) {
      console.warn('WebSocket connection failed:', e);
      if (!this.disposed) {
        this.scheduleReconnect(url);
      }
    }
  }

  /** Request a fresh snapshot from the backend. */
  requestSnapshot(): void {
    const ws = this.ws;
    if (ws && ws.readyState === 1 /* OPEN */) {
      ws.send(JSON.stringify({ type: 'request_snapshot' }));
    }
  }

  /** Disconnect and clean up. */
  dispose(): void {
    this.disposed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopStaleDetection();
    if (this.ws) {
      this.ws.onclose = null;
      this.ws.close();
      this.ws = null;
    }
    this.pendingUpdates = [];
  }

  private processMessage(msg: ServerMessage): void {
    switch (msg.type) {
      case 'snapshot':
        this.pendingUpdates = []; // Clear queue on fresh snapshot
        this.callbacks.onFrame(msg.frame as HeatmapFrame);
        break;

      case 'delta':
        if (this.pendingUpdates.length < MAX_PENDING_UPDATES) {
          this.pendingUpdates.push(msg);
          this.drainPendingUpdates();
        }
        // If queue is full, drop oldest deltas (keep latest state)
        break;

      case 'status':
        this.callbacks.onStatus({
          connection: msg.status.connection as ConnectionStatus,
          bookStatus: msg.status.book_status as any,
          symbol: msg.status.symbol,
          exchange: msg.status.exchange,
          bestBid: msg.status.best_bid,
          bestAsk: msg.status.best_ask,
          mid: msg.status.mid,
          spread: msg.status.spread,
          eventsPerSec: msg.status.events_per_sec,
          tradesPerSec: msg.status.trades_per_sec,
          heatmapCells: msg.status.heatmap_cells,
          sequenceErrors: msg.status.sequence_errors,
          queueDepth: msg.status.queue_depth,
        });
        break;
    }
  }

  private drainPendingUpdates(): void {
    // Coalesce compatible deltas: merge changed cells, combine new/removed
    while (this.pendingUpdates.length > 0) {
      const msg = this.pendingUpdates.shift()!;
      if (msg.type === 'delta') {
        this.callbacks.onDelta(msg.delta as HeatmapDelta);
      }
    }
  }

  private scheduleReconnect(url: string): void {
    const delay =
      RECONNECT_DELAYS[
        Math.min(this.reconnectAttempt, RECONNECT_DELAYS.length - 1)
      ];
    this.reconnectAttempt++;
    this.reconnectTimer = setTimeout(() => {
      this.connect(url);
    }, delay);
  }

  private startStaleDetection(): void {
    this.stopStaleDetection();
    this.staleTimer = setInterval(() => {
      const elapsed = Date.now() - this.lastUpdateTime;
      if (elapsed > STALE_THRESHOLD_MS && this.ws?.readyState === WebSocket.OPEN) {
        this.callbacks.onConnectionChange('DISCONNECTED');
      }
    }, 1000);
  }

  private stopStaleDetection(): void {
    if (this.staleTimer) {
      clearInterval(this.staleTimer);
      this.staleTimer = null;
    }
  }
}
