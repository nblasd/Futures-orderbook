/**
 * Tooltip — lightweight cell inspection overlay.
 * Only one active tooltip at a time.
 */

import React from 'react';
import { HeatmapCellSnapshot, ticksToPrice, ticksToQuantity } from '../types/heatmap';

export interface TooltipData {
  x: number;
  y: number;
  cell: HeatmapCellSnapshot;
  timestamp: number;
}

interface Props {
  data: TooltipData | null;
}

export const Tooltip: React.FC<Props> = ({ data }) => {
  if (!data) return null;

  const { x, y, cell, timestamp } = data;
  const price = ticksToPrice(cell.price_tick);
  const buyVol = ticksToQuantity(cell.executed_buy_volume);
  const sellVol = ticksToQuantity(cell.executed_sell_volume);
  const liqBid = ticksToQuantity(cell.resting_bid_liquidity);
  const liqAsk = ticksToQuantity(cell.resting_ask_liquidity);
  const added = ticksToQuantity(cell.liquidity_added);
  const removed = ticksToQuantity(cell.liquidity_removed);
  const delta = ticksToQuantity(cell.delta);
  const largeVol = ticksToQuantity(cell.large_trade_volume);

  const time = new Date(timestamp).toISOString().slice(11, 23);

  // Position tooltip to not overflow viewport
  const tooltipX = x + 16;
  const tooltipY = Math.max(0, y - 200);

  return (
    <div
      className="tooltip"
      style={{
        left: tooltipX,
        top: tooltipY,
      }}
    >
      <div className="tooltip-header">
        <span className="tooltip-price">{price.toFixed(2)}</span>
        <span className="tooltip-time">{time}</span>
      </div>
      <div className="tooltip-grid">
        <div className="tooltip-row">
          <span>Bid Liquidity</span>
          <span>{liqBid.toFixed(4)}</span>
        </div>
        <div className="tooltip-row">
          <span>Ask Liquidity</span>
          <span>{liqAsk.toFixed(4)}</span>
        </div>
        <div className="tooltip-divider" />
        <div className="tooltip-row">
          <span>Added</span>
          <span className="green">{added.toFixed(4)}</span>
        </div>
        <div className="tooltip-row">
          <span>Removed</span>
          <span className="red">{removed.toFixed(4)}</span>
        </div>
        <div className="tooltip-divider" />
        <div className="tooltip-row">
          <span>Buy Volume</span>
          <span className="green">{buyVol.toFixed(4)}</span>
        </div>
        <div className="tooltip-row">
          <span>Sell Volume</span>
          <span className="red">{sellVol.toFixed(4)}</span>
        </div>
        <div className="tooltip-row">
          <span>Delta</span>
          <span className={delta >= 0 ? 'green' : 'red'}>
            {delta.toFixed(4)}
          </span>
        </div>
        <div className="tooltip-row">
          <span>Trades</span>
          <span>{cell.trade_count}</span>
        </div>
        <div className="tooltip-row">
          <span>Large Trade Vol</span>
          <span>{largeVol.toFixed(4)}</span>
        </div>
        <div className="tooltip-divider" />
        <div className="tooltip-row">
          <span>Replenishments</span>
          <span>{cell.replenishment_count}</span>
        </div>
        <div className="tooltip-row">
          <span>Absorption</span>
          <span>{cell.absorption_candidate_count}</span>
        </div>
        <div className="tooltip-row">
          <span>Sweeps</span>
          <span>{cell.sweep_count}</span>
        </div>
        <div className="tooltip-row">
          <span>Pressure</span>
          <span>{ticksToQuantity(cell.pressure).toFixed(4)}</span>
        </div>
      </div>
    </div>
  );
};
