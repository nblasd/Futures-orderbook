/**
 * Controls — mode selector, time range, price aggregation, follow/pause.
 */

import React from 'react';
import { useStore } from '../hooks';
import type { HeatmapMode } from '../types/heatmap';
import { store } from '../state/store';

const MODES: { key: HeatmapMode; label: string; shortcut: string }[] = [
  { key: 'liquidity', label: 'Liquidity', shortcut: '1' },
  { key: 'execution', label: 'Execution', shortcut: '2' },
  { key: 'delta', label: 'Delta', shortcut: '3' },
  { key: 'absorption', label: 'Absorption', shortcut: '4' },
  { key: 'sweeps', label: 'Sweeps', shortcut: '5' },
  { key: 'pressure', label: 'Pressure', shortcut: '6' },
];

const TIME_RANGES = [
  { label: '1m', ms: 60_000 },
  { label: '5m', ms: 300_000 },
  { label: '15m', ms: 900_000 },
  { label: '30m', ms: 1_800_000 },
  { label: '1h', ms: 3_600_000 },
];

const AGGREGATIONS = [
  { label: '1 tick', value: 1 },
  { label: '5 ticks', value: 5 },
  { label: '10 ticks', value: 10 },
  { label: '25 ticks', value: 25 },
];

export const Controls: React.FC = () => {
  const mode = useStore((s) => s.mode);
  const viewport = useStore((s) => s.viewport);
  const timeRange = useStore((s) => s.timeRange);
  const priceAggregation = useStore((s) => s.priceAggregation);
  const setMode = (m: HeatmapMode) => store.setMode(m);
  const setTimeRange = (ms: number) => store.setTimeRange(ms);
  const setPriceAggregation = (v: number) => store.setPriceAggregation(v);
  const toggleFollow = () => store.toggleFollow();
  const togglePause = () => store.togglePause();
  const resetViewport = () => store.resetViewport();

  return (
    <div className="controls">
      {/* Mode buttons */}
      <div className="control-group">
        <div className="control-label">MODE</div>
        <div className="control-buttons">
          {MODES.map((m) => (
            <button
              key={m.key}
              className={`control-btn ${mode === m.key ? 'active' : ''}`}
              onClick={() => setMode(m.key)}
              title={`${m.label} (${m.shortcut})`}
            >
              {m.shortcut}
            </button>
          ))}
        </div>
      </div>

      {/* Time range */}
      <div className="control-group">
        <div className="control-label">TIME</div>
        <div className="control-buttons">
          {TIME_RANGES.map((tr) => (
            <button
              key={tr.ms}
              className={`control-btn ${timeRange === tr.ms ? 'active' : ''}`}
              onClick={() => setTimeRange(tr.ms)}
            >
              {tr.label}
            </button>
          ))}
        </div>
      </div>

      {/* Price aggregation */}
      <div className="control-group">
        <div className="control-label">AGG</div>
        <div className="control-buttons">
          {AGGREGATIONS.map((agg) => (
            <button
              key={agg.value}
              className={`control-btn ${priceAggregation === agg.value ? 'active' : ''}`}
              onClick={() => setPriceAggregation(agg.value)}
            >
              {agg.label}
            </button>
          ))}
        </div>
      </div>

      {/* Follow/Pause/Reset */}
      <div className="control-group">
        <button
          className={`control-btn wide ${viewport.follow ? 'active' : ''}`}
          onClick={toggleFollow}
          title="F"
        >
          FOLLOW
        </button>
        <button
          className={`control-btn wide ${viewport.paused ? 'active' : ''}`}
          onClick={togglePause}
          title="Space"
        >
          {viewport.paused ? 'PAUSED' : 'LIVE'}
        </button>
        <button
          className="control-btn wide"
          onClick={resetViewport}
          title="R"
        >
          RESET
        </button>
      </div>
    </div>
  );
};
