/**
 * MarketStatus — diagnostics panel showing connection, book, events.
 */

import React from 'react';
import { useStore } from '../hooks';

export const MarketStatus: React.FC = () => {
  const market = useStore((s) => s.market);
  const connection = useStore((s) => s.connection);
  const staleData = useStore((s) => s.staleData);

  return (
    <div className="market-status">
      <div className="status-section">
        <div className="status-row">
          <span className="status-key">Connection</span>
          <span className={`status-val ${connection.toLowerCase()}`}>
            {market.connection}
          </span>
        </div>
        <div className="status-row">
          <span className="status-key">Book</span>
          <span className="status-val">{market.bookStatus}</span>
        </div>
        <div className="status-row">
          <span className="status-key">Best Bid</span>
          <span className="status-val bid">
            {market.bestBid > 0 ? market.bestBid.toFixed(2) : '---'}
          </span>
        </div>
        <div className="status-row">
          <span className="status-key">Best Ask</span>
          <span className="status-val ask">
            {market.bestAsk > 0 ? market.bestAsk.toFixed(2) : '---'}
          </span>
        </div>
        <div className="status-row">
          <span className="status-key">Spread</span>
          <span className="status-val">
            {market.spread > 0 ? market.spread.toFixed(2) : '---'}
          </span>
        </div>
      </div>
      <div className="status-section">
        <div className="status-row">
          <span className="status-key">Events/s</span>
          <span className="status-val">{market.eventsPerSec}</span>
        </div>
        <div className="status-row">
          <span className="status-key">Trades/s</span>
          <span className="status-val">{market.tradesPerSec}</span>
        </div>
        <div className="status-row">
          <span className="status-key">Cells</span>
          <span className="status-val">{market.heatmapCells}</span>
        </div>
        <div className="status-row">
          <span className="status-key">Seq Errors</span>
          <span className="status-val">{market.sequenceErrors}</span>
        </div>
        <div className="status-row">
          <span className="status-key">Queue</span>
          <span className="status-val">{market.queueDepth}</span>
        </div>
      </div>
      {staleData && (
        <div className="stale-warning">⚠ STALE DATA</div>
      )}
    </div>
  );
};
