/**
 * Header component — displays BTCUSDT identity, connection status, and book data.
 */

import React from 'react';
import { useStore } from '../hooks';
import { ticksToPrice } from '../types/heatmap';

export const Header: React.FC = () => {
  const market = useStore((s) => s.market);
  const connection = useStore((s) => s.connection);

  const connectionColor =
    connection === 'LIVE'
      ? '#00ff88'
      : connection === 'DEMO'
        ? '#ffaa00'
        : connection === 'REPLAY'
          ? '#00aaff'
          : '#ff4444';

  return (
    <div className="header">
      <div className="header-left">
        <div className="header-title">
          <span className="header-symbol">BTCUSDT</span>
          <span className="header-contract">PERPETUAL</span>
        </div>
        <div className="header-exchange">{market.exchange}</div>
      </div>

      <div className="header-center">
        <div className="book-data">
          <div className="book-field">
            <span className="book-label">Best Bid</span>
            <span className="book-value bid">
              {market.bestBid > 0 ? market.bestBid.toFixed(2) : '-----'}
            </span>
          </div>
          <div className="book-field">
            <span className="book-label">Best Ask</span>
            <span className="book-value ask">
              {market.bestAsk > 0 ? market.bestAsk.toFixed(2) : '-----'}
            </span>
          </div>
          <div className="book-field">
            <span className="book-label">Mid</span>
            <span className="book-value">
              {market.mid > 0 ? market.mid.toFixed(2) : '-----'}
            </span>
          </div>
          <div className="book-field">
            <span className="book-label">Spread</span>
            <span className="book-value">
              {market.spread > 0 ? market.spread.toFixed(2) : '-----'}
            </span>
          </div>
        </div>
      </div>

      <div className="header-right">
        <div className="connection-status">
          <span
            className="status-dot"
            style={{ backgroundColor: connectionColor }}
          />
          <span className="status-label">{connection}</span>
        </div>
        {connection === 'DEMO' && (
          <div className="demo-badge">DEMO DATA</div>
        )}
      </div>
    </div>
  );
};
