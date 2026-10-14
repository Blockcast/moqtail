import assert from 'node:assert/strict';
import { test } from 'node:test';
import { nextRangeSeekTarget } from './next-range.ts';

const ranges = (...r: [number, number][]) => ({
  length: r.length,
  start: (i: number) => r[i][0],
  end: (i: number) => r[i][1],
});

test('jumps to a next range long enough to play', () => {
  assert.equal(nextRangeSeekTarget(ranges([0, 5], [5.3, 7]), 0, 0.6), 5.3);
});

test('waits when the next range is shorter than the minimum', () => {
  assert.equal(nextRangeSeekTarget(ranges([0, 5], [5.3, 5.5]), 0, 0.6), null);
});

test('accepts a next range exactly at the minimum', () => {
  assert.equal(nextRangeSeekTarget(ranges([0, 5], [6, 6.5]), 0, 0.5), 6);
});

test('returns null when there is no next range', () => {
  assert.equal(nextRangeSeekTarget(ranges([0, 5]), 0, 0.6), null);
});
