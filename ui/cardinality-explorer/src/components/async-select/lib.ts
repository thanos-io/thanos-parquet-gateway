import { useRef, useCallback } from 'react';

/**
 * Creates a debounced async function that delays execution and cancels pending calls.
 */
export function useDebounced<T extends unknown[], R>(
  fn: (...args: T) => Promise<R>,
  delay: number
): (...args: T) => Promise<R> {
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const latestResolveRef = useRef<((value: R) => void) | null>(null);

  return useCallback(
    (...args: T): Promise<R> => {
      return new Promise((resolve) => {
        // Clear previous timeout
        if (timeoutRef.current) {
          clearTimeout(timeoutRef.current);
        }

        // Store latest resolve
        latestResolveRef.current = resolve;

        // Set new timeout
        timeoutRef.current = setTimeout(async () => {
          const result = await fn(...args);
          // Only resolve if this is still the latest call
          if (latestResolveRef.current === resolve) {
            resolve(result);
          }
        }, delay);
      });
    },
    [fn, delay]
  );
}
