/**
 * Copyright 2026 The MOQtail Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** The subset of `TimeRanges` read here, so this stays testable outside a browser. */
export interface Ranges {
  readonly length: number;
  start(index: number): number;
  end(index: number): number;
}

/**
 * Start of the buffered range after `currentRangeIndex`, or null when there is none or it is
 * shorter than `minDuration`. Seeking into a range that short stalls again at once, so it is
 * better to wait for it to grow (at the live edge it is still being appended).
 */
export function nextRangeSeekTarget(
  buffered: Ranges,
  currentRangeIndex: number,
  minDuration: number,
): number | null {
  const next = currentRangeIndex + 1;
  if (next >= buffered.length) return null;
  const start = buffered.start(next);
  return buffered.end(next) - start < minDuration ? null : start;
}
