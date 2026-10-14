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

type CatalogObjectLike = {
  isEndOfGroup(): boolean;
  payload?: Uint8Array | null;
};

/**
 * The payload's own bytes. `MoqtObject.payload` is a zero-copy view into the
 * received chunk, so its `buffer` can also hold the subgroup header and sibling
 * objects; handing that whole buffer to the catalog parser feeds it garbage.
 */
function payloadBytes(payload: Uint8Array): ArrayBufferLike {
  if (payload.byteOffset === 0 && payload.byteLength === payload.buffer.byteLength) {
    return payload.buffer;
  }
  return payload.buffer.slice(payload.byteOffset, payload.byteOffset + payload.byteLength);
}

/**
 * Read objects until the first one carrying a payload and return exactly that
 * payload. `timeoutMs > 0` bounds the whole wait: a relay that parks the
 * catalog request would otherwise hang the player forever. On timeout the
 * stream is cancelled so a late object cannot leak.
 */
export async function readFirstCatalogPayload(
  source: ReadableStream<CatalogObjectLike>,
  timeoutMs = 0,
): Promise<ArrayBufferLike> {
  const reader = source.getReader();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline =
    timeoutMs > 0
      ? new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`Timed out after ${timeoutMs}ms waiting for catalog payload`)),
            timeoutMs,
          );
        })
      : undefined;
  let timedOut = false;
  try {
    while (true) {
      const result = deadline
        ? await Promise.race([reader.read(), deadline]).catch(e => {
            timedOut = true;
            throw e;
          })
        : await reader.read();
      if (result.done) throw new Error('Catalog stream closed unexpectedly while waiting for data');
      if (result.value.isEndOfGroup() || !result.value.payload) continue;
      return payloadBytes(result.value.payload);
    }
  } finally {
    clearTimeout(timer);
    if (timedOut) await reader.cancel('catalog payload timeout').catch(() => {});
    reader.releaseLock();
  }
}
