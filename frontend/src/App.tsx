/**
 * App — main application layout.
 *
 * Architecture:
 *   React UI components ← state store ← transport client / mock engine
 *   Heatmap Canvas ← renderer interface ← WebGPU or Canvas fallback
 */

import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Header } from './components/Header';
import { HeatmapView } from './components/HeatmapView';
import { PriceLadder } from './components/PriceLadder';
import { FlowPanel } from './components/FlowPanel';
import { Controls } from './components/Controls';
import { MarketStatus } from './components/MarketStatus';
import { Tooltip, TooltipData } from './components/Tooltip';
import { useKeyboardShortcuts } from './hooks';
import { store } from './state/store';
import { TransportClient } from './market/transport-client';
import './App.css';

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8080/ws';
const USE_BACKEND = import.meta.env.VITE_USE_BACKEND === 'true';

const App: React.FC = () => {
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const transportRef = useRef<TransportClient | null>(null);

  // Initialize keyboard shortcuts
  useKeyboardShortcuts();

  // Start demo mode on mount, or connect to backend
  useEffect(() => {
    if (USE_BACKEND) {
      // Connect to Rust backend
      const client = new TransportClient({
        onFrame: (frame) => store.receiveFrame(frame),
        onDelta: (delta) => store.receiveDelta(delta),
        onStatus: (status) => store.receiveStatus(status),
        onConnectionChange: (status) => store.setConnection(status),
      });
      transportRef.current = client;
      client.connect(WS_URL);
      return () => client.dispose();
    } else {
      // Demo mode
      store.startDemo(500);
      return () => store.stopDemo();
    }
  }, []);

  // Stale data detection
  useEffect(() => {
    const check = setInterval(() => {
      const elapsed = Date.now() - store.state.lastUpdateTime;
      store.state.staleData = elapsed > 5000;
    }, 1000);
    return () => clearInterval(check);
  }, []);

  const handleTooltip = useCallback((data: TooltipData | null) => {
    setTooltip(data);
  }, []);

  return (
    <div className="app">
      <Header />

      <div className="main-content">
        <div className="heatmap-area">
          <HeatmapView onTooltip={handleTooltip} />
          <Tooltip data={tooltip} />
        </div>
        <PriceLadder />
      </div>

      <FlowPanel />

      <div className="bottom-bar">
        <Controls />
        <MarketStatus />
      </div>

      <div className="keyboard-hints">
        Space: pause | F: follow | R: reset | +/-: zoom | 1-6: mode | ←→: pan
      </div>
    </div>
  );
};

export default App;
