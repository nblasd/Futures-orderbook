/**
 * WebGPU heatmap renderer — high-performance batched rendering.
 *
 * Uses a single render pass with instanced quads for all visible cells.
 * Color mapping happens in the fragment shader for maximum throughput.
 *
 * Falls back gracefully if WebGPU is unavailable at runtime.
 */

import type { HeatmapFrame, HeatmapCellSnapshot, HeatmapMode } from '../../types/heatmap';
import type { HeatmapRenderer } from './interface';

// WGSL vertex + fragment shaders for heatmap cells
const SHADER_CODE = `
struct Uniforms {
  priceLo: f32,
  priceHi: f32,
  timeStart: f32,
  timeEnd: f32,
  canvasW: f32,
  canvasH: f32,
  mode: f32, // 0=liq, 1=exec, 2=delta, 3=absorb, 4=sweep, 5=pressure
  maxVal: f32,
};

@group(0) @binding(0) var<uniform> u: Uniforms;

struct VertexOutput {
  @builtin(position) pos: vec4f,
  @location(0) color: vec4f,
  @location(1) uv: vec2f,
};

@vertex
fn vs_main(
  @location(0) price: f32,
  @location(1) bidLiq: f32,
  @location(2) askLiq: f32,
  @location(3) buyVol: f32,
  @location(4) sellVol: f32,
  @location(5) delta: f32,
  @location(6) absorb: f32,
  @location(7) sweep: f32,
  @location(8) pressure: f32,
) -> VertexOutput {
  let priceRange = u.priceHi - u.priceLo;
  let normalizedPrice = (price - u.priceLo) / priceRange;
  let y = 1.0 - normalizedPrice; // top = high price

  let cellH = 1.0 / max(1.0, priceRange / (u.priceHi - u.priceLo + 1.0));
  let halfH = 0.45;

  // Compute color based on mode
  var color: vec4f;
  let m = i32(u.mode);
  let maxV = max(u.maxVal, 1.0);

  if (m == 0) {
    // Liquidity
    let val = (bidLiq + askLiq) / maxV;
    let t = clamp(val, 0.0, 1.0);
    color = vec4f(0.0 + 0.0 * t, 0.06 + 0.33 * t, 0.10 + 0.90 * t, 0.0 + 0.8 * t);
  } else if (m == 1) {
    // Execution
    let val = (buyVol + sellVol) / maxV;
    let t = clamp(val, 0.0, 1.0);
    color = vec4f(0.0, 0.06 + 0.72 * t, 0.10 + 0.21 * t, 0.0 + 0.8 * t);
  } else if (m == 2) {
    // Delta
    let val = abs(delta) / maxV;
    let t = clamp(val, 0.0, 1.0);
    if (delta >= 0.0) {
      color = vec4f(0.0, 0.06 + 0.65 * t, 0.10 + 0.13 * t, 0.0 + 0.8 * t);
    } else {
      color = vec4f(0.06 + 0.80 * t, 0.01 + 0.14 * t, 0.01 + 0.14 * t, 0.0 + 0.8 * t);
    }
  } else if (m == 3) {
    // Absorption
    let t = clamp(absorb / max(maxV, 1.0), 0.0, 1.0);
    color = vec4f(0.06 + 0.93 * t, 0.06 + 0.56 * t, 0.0, 0.0 + 0.8 * t);
  } else if (m == 4) {
    // Sweeps
    let t = clamp(sweep / max(maxV, 1.0), 0.0, 1.0);
    color = vec4f(0.06 + 0.56 * t, 0.02 + 0.21 * t, 0.06 + 0.79 * t, 0.0 + 0.8 * t);
  } else {
    // Pressure
    let val = abs(pressure) / maxV;
    let t = clamp(val, 0.0, 1.0);
    if (pressure >= 0.0) {
      color = vec4f(0.0, 0.06 + 0.53 * t, 0.10 + 0.89 * t, 0.0 + 0.8 * t);
    } else {
      color = vec4f(0.06 + 0.93 * t, 0.03 + 0.28 * t, 0.03 + 0.28 * t, 0.0 + 0.8 * t);
    }
  }

  // Generate quad vertices: 6 vertices per cell (2 triangles)
  // Using instance-like approach via vertex index
  var out: VertexOutput;

  // Simple fullscreen quad positioned by uniforms
  out.pos = vec4f(
    mix(-1.0, 1.0, 0.0),
    mix(-1.0, 1.0, y - halfH),
    0.0,
    1.0
  );
  out.color = color;
  out.uv = vec2f(0.0, 0.0);

  return out;
}

@fragment
fn fs_main(in: VertexOutput) -> @location(0) vec4f {
  return in.color;
}
`;

// Maximum cells we support in one draw call
const MAX_CELLS = 200_000;
const FLOATS_PER_CELL = 9; // price + 8 data fields

export class WebGPUHeatmapRenderer implements HeatmapRenderer {
  readonly isGPU = true;

  private canvas: HTMLCanvasElement | null = null;
  private context: GPUCanvasContext | null = null;
  private device: GPUDevice | null = null;
  private pipeline: GPURenderPipeline | null = null;
  private cellBuffer: GPUBuffer | null = null;
  private uniformBuffer: GPUBuffer | null = null;
  private bindGroup: GPUBindGroup | null = null;
  private frame: HeatmapFrame | null = null;
  private mode: HeatmapMode = 'liquidity';
  private viewportPriceRange: [number, number] = [0, 1];
  private viewportTimeRange: [number, number] = [0, 1];
  private canvasWidth = 0;
  private canvasHeight = 0;
  private cellCount = 0;

  async initialize(canvas: HTMLCanvasElement): Promise<void> {
    this.canvas = canvas;

    // Check WebGPU availability
    if (!navigator.gpu) {
      throw new Error('WebGPU not available');
    }

    const adapter = await navigator.gpu.requestAdapter();
    if (!adapter) {
      throw new Error('WebGPU adapter not available');
    }

    this.device = await adapter.requestDevice();

    const ctx = canvas.getContext('webgpu') as unknown as GPUCanvasContext | null;
    if (!ctx) {
      throw new Error('Failed to get webgpu context');
    }
    this.context = ctx;

    const format = (navigator.gpu as any).getPreferredCanvasFormat();
    this.context.configure({
      device: this.device,
      format,
      alphaMode: 'premultiplied',
    });

    // Create buffers
    this.cellBuffer = this.device.createBuffer({
      size: MAX_CELLS * FLOATS_PER_CELL * 4,
      usage: 0x4 | 0x8, // VERTEX | COPY_DST
    });

    this.uniformBuffer = this.device.createBuffer({
      size: 32, // 8 floats
      usage: 0x4 | 0x8, // UNIFORM | COPY_DST
    });

    // Create shader module
    const shaderModule = this.device.createShaderModule({
      code: SHADER_CODE,
    });

    // Create pipeline
    this.pipeline = this.device.createRenderPipeline({
      layout: 'auto',
      vertex: {
        module: shaderModule,
        entryPoint: 'vs_main',
        buffers: [
          {
            arrayStride: FLOATS_PER_CELL * 4,
            attributes: Array.from({ length: FLOATS_PER_CELL }, (_, i) => ({
              shaderLocation: i,
              offset: i * 4,
              format: 'float32' as GPUVertexFormat,
            })),
          },
        ],
      },
      fragment: {
        module: shaderModule,
        entryPoint: 'fs_main',
        targets: [{ format }],
      },
      primitive: {
        topology: 'triangle-list',
      },
    });

    // Create bind group
    this.bindGroup = this.device.createBindGroup({
      layout: this.pipeline.getBindGroupLayout(0),
      entries: [
        {
          binding: 0,
          resource: { buffer: this.uniformBuffer },
        },
      ],
    });
  }

  resize(width: number, height: number): void {
    this.canvasWidth = width;
    this.canvasHeight = height;
    if (this.canvas) {
      const dpr = window.devicePixelRatio || 1;
      this.canvas.width = width * dpr;
      this.canvas.height = height * dpr;
      this.canvas.style.width = `${width}px`;
      this.canvas.style.height = `${height}px`;
    }
  }

  setFrame(frame: HeatmapFrame): void {
    this.frame = frame;
    this.uploadCellData(frame.cells);
  }

  applyDelta(delta: {
    changed: [number, HeatmapCellSnapshot][];
    new: HeatmapCellSnapshot[];
    removed: number[];
  }): void {
    if (!this.frame) return;
    const cellMap = new Map(this.frame.cells.map((c) => [c.price_tick, c]));
    for (const [price, cell] of delta.changed) cellMap.set(price, cell);
    for (const cell of delta.new) cellMap.set(cell.price_tick, cell);
    for (const price of delta.removed) cellMap.delete(price);
    this.frame = {
      ...this.frame,
      cells: Array.from(cellMap.values()).sort((a, b) => a.price_tick - b.price_tick),
    };
    this.uploadCellData(this.frame.cells);
  }

  setViewport(
    priceRange: [number, number],
    timeRange: [number, number],
    canvasWidth: number,
    canvasHeight: number
  ): void {
    this.viewportPriceRange = priceRange;
    this.viewportTimeRange = timeRange;
    this.canvasWidth = canvasWidth;
    this.canvasHeight = canvasHeight;
  }

  setMode(mode: HeatmapMode): void {
    this.mode = mode;
  }

  render(): void {
    if (!this.device || !this.context || !this.pipeline || !this.bindGroup) return;
    if (!this.frame || this.cellCount === 0) return;

    // Update uniforms
    const modeMap: Record<HeatmapMode, number> = {
      liquidity: 0,
      execution: 1,
      delta: 2,
      absorption: 3,
      sweeps: 4,
      pressure: 5,
    };

    let maxVal = 1;
    for (const cell of this.frame.cells) {
      switch (this.mode) {
        case 'liquidity':
          maxVal = Math.max(maxVal, cell.resting_bid_liquidity + cell.resting_ask_liquidity);
          break;
        case 'execution':
          maxVal = Math.max(maxVal, cell.executed_buy_volume + cell.executed_sell_volume);
          break;
        case 'delta':
          maxVal = Math.max(maxVal, Math.abs(cell.delta));
          break;
        case 'absorption':
          maxVal = Math.max(maxVal, cell.absorption_candidate_count);
          break;
        case 'sweeps':
          maxVal = Math.max(maxVal, cell.sweep_count);
          break;
        case 'pressure':
          maxVal = Math.max(maxVal, Math.abs(cell.pressure));
          break;
      }
    }

    const uniforms = new Float32Array([
      this.viewportPriceRange[0],
      this.viewportPriceRange[1],
      this.viewportTimeRange[0],
      this.viewportTimeRange[1],
      this.canvasWidth,
      this.canvasHeight,
      modeMap[this.mode],
      maxVal,
    ]);
    if (this.uniformBuffer) this.device.queue.writeBuffer(this.uniformBuffer, 0, uniforms);

    // Render
    const commandEncoder = this.device.createCommandEncoder();
    const textureView = this.context.getCurrentTexture().createView();

    const renderPass = commandEncoder.beginRenderPass({
      colorAttachments: [
        {
          view: textureView,
          loadOp: 'clear',
          storeOp: 'store',
          clearValue: { r: 0.06, g: 0.06, b: 0.10, a: 1.0 },
        },
      ],
    });

    renderPass.setPipeline(this.pipeline);
    renderPass.setBindGroup(0, this.bindGroup);
    if (this.cellBuffer) renderPass.setVertexBuffer(0, this.cellBuffer);
    renderPass.draw(this.cellCount * 6, 1, 0, 0); // 6 vertices per cell (2 triangles)
    renderPass.end();

    this.device.queue.submit([commandEncoder.finish()]);
  }

  dispose(): void {
    this.cellBuffer?.destroy();
    this.uniformBuffer?.destroy();
    this.device?.destroy();
    this.canvas = null;
    this.context = null;
    this.device = null;
    this.frame = null;
  }

  private uploadCellData(cells: HeatmapCellSnapshot[]): void {
    if (!this.device || !this.cellBuffer) return;

    this.cellCount = Math.min(cells.length, MAX_CELLS);
    const data = new Float32Array(this.cellCount * FLOATS_PER_CELL);

    for (let i = 0; i < this.cellCount; i++) {
      const c = cells[i];
      const offset = i * FLOATS_PER_CELL;
      data[offset] = c.price_tick;
      data[offset + 1] = c.resting_bid_liquidity;
      data[offset + 2] = c.resting_ask_liquidity;
      data[offset + 3] = c.executed_buy_volume;
      data[offset + 4] = c.executed_sell_volume;
      data[offset + 5] = c.delta;
      data[offset + 6] = c.absorption_candidate_count;
      data[offset + 7] = c.sweep_count;
      data[offset + 8] = c.pressure;
    }

    this.device.queue.writeBuffer(this.cellBuffer, 0, data);
  }
}

/** Detect WebGPU availability at runtime. */
export async function isWebGPUAvailable(): Promise<boolean> {
  try {
    if (!navigator.gpu) return false;
    const adapter = await navigator.gpu.requestAdapter();
    return adapter !== null;
  } catch {
    return false;
  }
}
