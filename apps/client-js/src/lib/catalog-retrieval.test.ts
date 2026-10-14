import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFirstCatalogPayload } from './catalog-retrieval.ts';

const obj = (payload?: Uint8Array, eog = false) => ({ isEndOfGroup: () => eog, payload });
const streamOf = (...objs: ReturnType<typeof obj>[]) =>
  new ReadableStream({
    start(c) {
      for (const o of objs) c.enqueue(o);
      c.close();
    },
  });

test('returns only the payload bytes of a view into a larger chunk', async () => {
  const chunk = new Uint8Array([0xde, 0xad, 1, 2, 3, 0xbe, 0xef]);
  const buf = await readFirstCatalogPayload(streamOf(obj(chunk.subarray(2, 5))));
  assert.deepEqual([...new Uint8Array(buf)], [1, 2, 3]);
});

test('skips end-of-group and payload-less objects', async () => {
  const buf = await readFirstCatalogPayload(
    streamOf(obj(undefined, true), obj(undefined), obj(new Uint8Array([7]))),
  );
  assert.deepEqual([...new Uint8Array(buf)], [7]);
});

test('rejects when the stream closes before any payload', async () => {
  await assert.rejects(
    readFirstCatalogPayload(streamOf(obj(undefined, true))),
    /closed unexpectedly/,
  );
});

test('times out and cancels a stream that never delivers', async () => {
  let cancelled = false;
  const stalled = new ReadableStream({
    cancel() {
      cancelled = true;
    },
  });
  await assert.rejects(readFirstCatalogPayload(stalled, 20), /Timed out after 20ms/);
  assert.equal(cancelled, true);
});
