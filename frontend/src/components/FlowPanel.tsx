/**
 * FlowPanel — bottom panel showing delta, CVD, buy/sell volume, trade count.
 * All values come directly from the backend — no independent CVD calculation.
 */

import React from 'react';
import { useStore } from '../hooks';
import { ticksToQuantity } from '../types/heatmap';

export const FlowPanel: React.FC = () => {
  const frame = useStore((s) => s.frame);
  const market = useStore((s) => s.market);

  if (!frame) {
    return (
      <div className="flow-panel">
        <div className="flow-label">ORDER FLOW</div>
        <div className="flow-empty">Waiting for data...</div>
      </div>
    );
  }

  const s = frame.summary;
  const buyVol = ticksToQuantity(s.total_executed_buy);
  const sellVol = ticksToQuantity(s.total_executed_sell);
  const delta = ticksToQuantity(s.total_delta);

  return (
    <div className="flow-panel">
      <div className="flow-label">ORDER FLOW</div>
      <div className="flow-fields">
        <div className="flow-field">
          <span className="flow-name">Delta</span>
          <span className={`flow-value ${delta >= 0 ? 'green' : 'red'}`}>
            {delta.toFixed(4)}
          </span>
        </div>
        <div className="flow-field">
          <span className="flow-name">Buy Vol</span>
          <span className="flow-value green">{buyVol.toFixed(4)}</span>
        </div>
        <div className="flow-field">
          <span className="flow-name">Sell Vol</span>
          <span className="flow-value red">{sellVol.toFixed(4)}</span>
        </div>
        <div className="flow-field">
          <span className="flow-name">Trades</span>
          <span className="flow-value">{s.total_trade_count}</span>
        </div>
        <div className="flow-field">
          <span className="flow-name">Prices</span>
          <span className="flow-value">{s.total_price_levels}</span>
        </div>
        <div className="flow-field">
          <span className="flow-name">Replenish</span>
          <span className="flow-value">{s.total_replenishment_count}</span>
        </div>
        <div className="flow-field">
          <span className="flow-name">Absorb</span>
          <span className="flow-value">{s.total_absorption_candidate_count}</span>
        </div>
        <div className="flow-field">
          <span className="flow-name">Sweeps</span>
          <span className="flow-value">{s.total_sweep_count}</span>
        </div>
      </div>
      <div className="flow-stats">
        <span>Events: {market.eventsPerSec}/s</span>
        <span>Trades: {market.tradesPerSec}/s</span>
        <span>Cells: {market.heatmapCells}</span>
      </div>
    </div>
  );
};
