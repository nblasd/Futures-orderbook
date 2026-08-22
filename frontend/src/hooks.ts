/**
 * React hooks for the state store.
 */

import { useEffect, useSyncExternalStore } from 'react';
import { store, AppState } from './state/store';

/** Subscribe to the store and get a slice of state. */
export function useStore<T>(selector: (state: AppState) => T): T {
  return useSyncExternalStore(
    (cb) => store.subscribe(cb),
    () => selector(store.state),
    () => selector(store.state)
  );
}

/** Run keyboard shortcuts. */
export function useKeyboardShortcuts(): void {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Don't trigger if user is typing in an input
      if (
        e.target instanceof HTMLInputElement ||
        e.target instanceof HTMLTextAreaElement
      )
        return;

      switch (e.key) {
        case ' ':
          e.preventDefault();
          store.togglePause();
          break;
        case 'f':
        case 'F':
          store.toggleFollow();
          break;
        case 'r':
        case 'R':
          store.resetViewport();
          break;
        case '+':
        case '=':
          store.zoomPrices(0.8);
          break;
        case '-':
          store.zoomPrices(1.25);
          break;
        case '1':
          store.setMode('liquidity');
          break;
        case '2':
          store.setMode('execution');
          break;
        case '3':
          store.setMode('delta');
          break;
        case '4':
          store.setMode('absorption');
          break;
        case '5':
          store.setMode('sweeps');
          break;
        case '6':
          store.setMode('pressure');
          break;
        case 'ArrowLeft':
          store.panTime(-5000);
          break;
        case 'ArrowRight':
          store.panTime(5000);
          break;
        case 'ArrowUp':
          store.zoomPrices(0.9);
          break;
        case 'ArrowDown':
          store.zoomPrices(1.1);
          break;
      }
    };

    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);
}
