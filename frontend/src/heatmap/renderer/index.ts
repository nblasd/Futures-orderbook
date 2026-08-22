/**
 * Renderer factory — detects WebGPU capability and returns
 * the appropriate renderer implementation.
 */

import type { HeatmapRenderer } from './interface';
import { isWebGPUAvailable } from './webgpu-renderer';

let cachedResult: { available: boolean; checked: boolean } = {
  available: false,
  checked: false,
};

/** Check if WebGPU is available (cached after first call). */
export async function checkWebGPU(): Promise<boolean> {
  if (cachedResult.checked) return cachedResult.available;
  cachedResult.available = await isWebGPUAvailable();
  cachedResult.checked = true;
  return cachedResult.available;
}

/**
 * Create the best available renderer.
 * Tries WebGPU first, falls back to Canvas 2D.
 */
export async function createRenderer(): Promise<HeatmapRenderer> {
  const gpuAvailable = await checkWebGPU();

  if (gpuAvailable) {
    try {
      const { WebGPUHeatmapRenderer } = await import('./webgpu-renderer');
      const renderer = new WebGPUHeatmapRenderer();
      return renderer;
    } catch (e) {
      console.warn('WebGPU renderer failed to initialize, falling back:', e);
    }
  }

  const { FallbackHeatmapRenderer } = await import('./canvas-fallback');
  return new FallbackHeatmapRenderer();
}

export type { HeatmapRenderer } from './interface';
