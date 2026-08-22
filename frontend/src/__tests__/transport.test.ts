import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { TransportClient } from '../market/transport-client';

// Mock WebSocket
class MockWebSocket {
  static instances: MockWebSocket[] = [];
  url: string;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readyState = 1; // OPEN
  sent: string[] = [];

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
    // Simulate async open
    setTimeout(() => this.onopen?.(), 0);
  }

  send(data: string) {
    this.sent.push(data);
  }

  close() {
    this.readyState = 3;
    setTimeout(() => this.onclose?.(), 0);
  }

  // Helper to simulate receiving a message
  simulateMessage(data: object) {
    this.onmessage?.({ data: JSON.stringify(data) });
  }

  // Helper to simulate disconnect
  simulateDisconnect() {
    this.readyState = 3;
    this.onclose?.();
  }
}

// Replace global WebSocket
const OriginalWebSocket = globalThis.WebSocket;

beforeEach(() => {
  MockWebSocket.instances = [];
  (globalThis as any).WebSocket = MockWebSocket;
});

afterEach(() => {
  (globalThis as any).WebSocket = OriginalWebSocket;
});

describe('TransportClient', () => {
  it('connects and receives initial snapshot', () => {
    const onFrame = vi.fn();
    const onConnectionChange = vi.fn();

    const client = new TransportClient({
      onFrame,
      onDelta: vi.fn(),
      onStatus: vi.fn(),
      onConnectionChange,
    });

    client.connect('ws://test/ws');

    // Simulate server sending snapshot
    const ws = MockWebSocket.instances[0];
    ws.simulateMessage({
      type: 'snapshot',
      frame: {
        timestamp: 1000,
        visible_price_range: [7720000000000, 7740000000000],
        time_range: [990000, 1001000],
        cells: [],
        summary: {
          total_price_levels: 0,
          total_buckets: 1,
          total_executed_buy: 0,
          total_executed_sell: 0,
          total_delta: 0,
          total_trade_count: 0,
          total_liquidity_added: 0,
          total_liquidity_removed: 0,
          total_large_trade_volume: 0,
          total_replenishment_count: 0,
          total_absorption_candidate_count: 0,
          total_sweep_count: 0,
        },
      },
    });

    expect(onFrame).toHaveBeenCalledTimes(1);
    expect(onFrame).toHaveBeenCalledWith(
      expect.objectContaining({ timestamp: 1000 })
    );
    client.dispose();
  });

  it('receives delta updates', () => {
    const onDelta = vi.fn();

    const client = new TransportClient({
      onFrame: vi.fn(),
      onDelta,
      onStatus: vi.fn(),
      onConnectionChange: vi.fn(),
    });

    client.connect('ws://test/ws');
    const ws = MockWebSocket.instances[0];

    // First send a snapshot
    ws.simulateMessage({
      type: 'snapshot',
      frame: {
        timestamp: 1000,
        visible_price_range: [7720000000000, 7740000000000],
        time_range: [990000, 1001000],
        cells: [],
        summary: {
          total_price_levels: 0,
          total_buckets: 1,
          total_executed_buy: 0,
          total_executed_sell: 0,
          total_delta: 0,
          total_trade_count: 0,
          total_liquidity_added: 0,
          total_liquidity_removed: 0,
          total_large_trade_volume: 0,
          total_replenishment_count: 0,
          total_absorption_candidate_count: 0,
          total_sweep_count: 0,
        },
      },
    });

    // Then send a delta
    ws.simulateMessage({
      type: 'delta',
      delta: {
        changed: [],
        new: [
          {
            price_tick: 7730000000000,
            resting_bid_liquidity: 100000000,
            resting_ask_liquidity: 200000000,
            liquidity_added: 0,
            liquidity_removed: 0,
            executed_buy_volume: 0,
            executed_sell_volume: 0,
            delta: 0,
            trade_count: 0,
            large_trade_volume: 0,
            replenishment_count: 0,
            absorption_candidate_count: 0,
            sweep_count: 0,
            pressure: 0,
          },
        ],
        removed: [],
        summary_delta: {
          total_executed_buy: 0,
          total_executed_sell: 0,
          total_delta: 0,
          total_trade_count: 0,
          total_liquidity_added: 0,
          total_liquidity_removed: 0,
          total_large_trade_volume: 0,
          total_replenishment_count: 0,
          total_absorption_candidate_count: 0,
          total_sweep_count: 0,
        },
      },
    });

    expect(onDelta).toHaveBeenCalledTimes(1);
    client.dispose();
  });

  it('receives status updates', () => {
    const onStatus = vi.fn();

    const client = new TransportClient({
      onFrame: vi.fn(),
      onDelta: vi.fn(),
      onStatus,
      onConnectionChange: vi.fn(),
    });

    client.connect('ws://test/ws');
    const ws = MockWebSocket.instances[0];

    ws.simulateMessage({
      type: 'status',
      status: {
        connection: 'LIVE',
        book_status: 'READY',
        symbol: 'BTCUSDT',
        exchange: 'Binance USDⓈ-M Futures',
        best_bid: 77300.0,
        best_ask: 77300.1,
        mid: 77300.05,
        spread: 0.1,
        events_per_sec: 100,
        trades_per_sec: 10,
        heatmap_cells: 500,
        sequence_errors: 0,
        queue_depth: 0,
      },
    });

    expect(onStatus).toHaveBeenCalledTimes(1);
    expect(onStatus).toHaveBeenCalledWith(
      expect.objectContaining({ bestBid: 77300.0 })
    );
    client.dispose();
  });

  it('requests snapshot from server', async () => {
    const client = new TransportClient({
      onFrame: vi.fn(),
      onDelta: vi.fn(),
      onStatus: vi.fn(),
      onConnectionChange: vi.fn(),
    });

    client.connect('ws://test/ws');
    // Wait for mock WebSocket onopen callback
    await new Promise((r) => setTimeout(r, 10));
    const ws = MockWebSocket.instances[0];

    client.requestSnapshot();

    expect(ws.sent).toContainEqual(
      JSON.stringify({ type: 'request_snapshot' })
    );
    client.dispose();
  });

  it('cleans up on dispose', async () => {
    const client = new TransportClient({
      onFrame: vi.fn(),
      onDelta: vi.fn(),
      onStatus: vi.fn(),
      onConnectionChange: vi.fn(),
    });

    client.connect('ws://test/ws');
    await new Promise((r) => setTimeout(r, 10));
    client.dispose();

    // Should not throw after dispose
    expect(() => client.requestSnapshot()).not.toThrow();
  });
});
