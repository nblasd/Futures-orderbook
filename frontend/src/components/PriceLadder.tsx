/**
 * PriceLadder — right-side panel showing price levels with
 * bid/ask liquidity bars. Current price highlighted.
 */

import React from 'react';
import { useStore } from '../hooks';
import { ticksToPrice, ticksToQuantity } from '../types/heatmap';

export const PriceLadder: React.FC = () => {
  const frame = useStore((s) => s.frame);
  const viewport = useStore((s) => s.viewport);

  if (!frame || frame.cells.length === 0) {
    return (
      <div className="price-ladder">
        <div className="ladder-header">PRICE</div>
        <div className="ladder-empty">No data</div>
      </div>
    );
  }

  // Find max liquidity for bar scaling
  let maxLiq = 1;
  for (const cell of frame.cells) {
    const liq = cell.resting_bid_liquidity + cell.resting_ask_liquidity;
    if (liq > maxLiq) maxLiq = liq;
  }

  // Show cells visible in the viewport
  const [priceLo, priceHi] = viewport.priceRange;
  const visible = frame.cells.filter(
    (c) => c.price_tick >= priceLo && c.price_tick <= priceHi
  );

  // Limit display to avoid excessive rendering
  const displayCells = visible.length > 40
    ? visible.filter((_, i) => i % Math.ceil(visible.length / 40) === 0)
    : visible;

  return (
    <div className="price-ladder">
      <div className="ladder-header">PRICE</div>
      <div className="ladder-body">
        {displayCells.map((cell) => {
          const price = ticksToPrice(cell.price_tick);
          const bidLiq = ticksToQuantity(cell.resting_bid_liquidity);
          const askLiq = ticksToQuantity(cell.resting_ask_liquidity);
          const bidPct = (cell.resting_bid_liquidity / maxLiq) * 100;
          const askPct = (cell.resting_ask_liquidity / maxLiq) * 100;

          return (
            <div
              key={cell.price_tick}
              className="ladder-row"
            >
              <div className="ladder-bid-bar" style={{ width: `${bidPct}%` }} />
              <div className="ladder-price">{price.toFixed(2)}</div>
              <div className="ladder-ask-bar" style={{ width: `${askPct}%` }} />
              <div className="ladder-bid-vol">
                {bidLiq > 0 ? bidLiq.toFixed(2) : ''}
              </div>
              <div className="ladder-ask-vol">
                {askLiq > 0 ? askLiq.toFixed(2) : ''}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
