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

/**
 * iPhone Safari (iOS 17.1+) exposes only `ManagedMediaSource`, not
 * `MediaSource`. Construction and `isTypeSupported()` must use the same
 * constructor, so a MIME probe never answers for an API the player is not using.
 * `MediaSource` wins where both exist, which keeps desktop behaviour unchanged.
 */
export interface MediaSourceCtor {
  new (): MediaSource;
  isTypeSupported(type: string): boolean;
}

export interface MediaSourceApi {
  ctor: MediaSourceCtor;
  /** ManagedMediaSource only opens on an element with remote playback disabled. */
  managed: boolean;
}

const asCtor = (v: unknown): MediaSourceCtor | null =>
  typeof v === 'function' && typeof (v as MediaSourceCtor).isTypeSupported === 'function'
    ? (v as MediaSourceCtor)
    : null;

export function resolveMediaSourceApi(
  scope: { MediaSource?: unknown; ManagedMediaSource?: unknown } = globalThis,
): MediaSourceApi {
  const unmanaged = asCtor(scope.MediaSource);
  if (unmanaged) return { ctor: unmanaged, managed: false };
  const managed = asCtor(scope.ManagedMediaSource);
  if (managed) return { ctor: managed, managed: true };
  throw new Error('This browser exposes neither MediaSource nor ManagedMediaSource');
}
