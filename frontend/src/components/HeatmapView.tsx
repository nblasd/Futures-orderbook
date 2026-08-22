/**
 * HeatmapView — the main canvas-based heatmap visualization.
 *
 * Uses the renderer abstraction (WebGPU or Canvas fallback).
 * All rendering happens through the canvas, no DOM-per-cell.
 */

import React, { useRef, useEffect, useCallback, useState } from 'react';
import { useStore } from '../hooks';
import { createRenderer, HeatmapRenderer } from '../heatmap/renderer';
import { ticksToPrice } from '../types/heatmap';
import { TooltipData } from './Tooltip';

interface Props {
  onTooltip: (data: TooltipData | null) => void;
}

export const HeatmapView: React.FC<Props> = ({ onTooltip }) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const rendererRef = useRef<HeatmapRenderer | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const animFrameRef = useRef<number>(0);

  const frame = useStore((s) => s.frame);
  const mode = useStore((s) => s.mode);
  const viewport = useStore((s) => s.viewport);

  const [rendererType, setRendererType] = useState<string>('initializing');

  // Initialize renderer
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    let disposed = false;

    const init = async () => {
      try {
        const renderer = await createRenderer();
        if (disposed) {
          renderer.dispose();
          return;
        }
        rendererRef.current = renderer;
        setRendererType(renderer.isGPU ? 'WebGPU' : 'Canvas 2D');

        const rect = canvas.parentElement?.getBoundingClientRect();
        if (rect) {
          await renderer.initialize(canvas);
          renderer.resize(rect.width, rect.height);
        }
      } catch (e) {
        console.error('Renderer init failed:', e);
        setRendererType('error');
      }
    };

    init();

    return () => {
      disposed = true;
      rendererRef.current?.dispose();
      rendererRef.current = null;
    };
  }, []);

  // Handle resize
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        rendererRef.current?.resize(width, height);
      }
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, []);

  // Update mode
  useEffect(() => {
    rendererRef.current?.setMode(mode);
  }, [mode]);

  // Render loop
  useEffect(() => {
    const render = () => {
      const renderer = rendererRef.current;
      if (renderer && frame) {
        renderer.setViewport(
          viewport.priceRange,
          viewport.timeRange,
          containerRef.current?.clientWidth ?? 800,
          containerRef.current?.clientHeight ?? 600
        );
        renderer.setFrame(frame);
        renderer.render();
      }
      animFrameRef.current = requestAnimationFrame(render);
    };
    animFrameRef.current = requestAnimationFrame(render);
    return () => cancelAnimationFrame(animFrameRef.current);
  }, [frame, viewport]);

  // Mouse hover → tooltip
  const handleMouseMove = useCallback(
    (e: React.MouseEvent<HTMLCanvasElement>) => {
      if (!frame) return;
      const rect = e.currentTarget.getBoundingClientRect();
      const y = e.clientY - rect.top;
      const height = rect.height;

      // Map Y to price
      const [priceLo, priceHi] = viewport.priceRange;
      const priceRange = priceHi - priceLo;
      const normalizedY = 1 - y / height;
      const priceTick = Math.round(priceLo + normalizedY * priceRange);

      // Find nearest cell
      let nearest = frame.cells[0];
      let minDist = Infinity;
      for (const cell of frame.cells) {
        const dist = Math.abs(cell.price_tick - priceTick);
        if (dist < minDist) {
          minDist = dist;
          nearest = cell;
        }
      }

      if (nearest && minDist < priceRange * 0.05) {
        onTooltip({
          x: e.clientX,
          y: e.clientY,
          cell: nearest,
          timestamp: frame.timestamp,
        });
      } else {
        onTooltip(null);
      }
    },
    [frame, viewport, onTooltip]
  );

  const handleMouseLeave = useCallback(() => {
    onTooltip(null);
  }, [onTooltip]);

  return (
    <div className="heatmap-container" ref={containerRef}>
      <canvas
        ref={canvasRef}
        className="heatmap-canvas"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
      />
      <div className="renderer-badge">{rendererType}</div>
    </div>
  );
};
