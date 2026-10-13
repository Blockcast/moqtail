import assert from 'node:assert/strict';
import { test } from 'node:test';
import { resolveMediaSourceApi } from './media-source.ts';

class Mse {
  static isTypeSupported() {
    return true;
  }
}
class Managed {
  static isTypeSupported() {
    return false;
  }
}

test('prefers MediaSource where both exist (desktop Safari)', () => {
  const api = resolveMediaSourceApi({ MediaSource: Mse, ManagedMediaSource: Managed });
  assert.equal(api.ctor, Mse);
  assert.equal(api.managed, false);
});

test('falls back to ManagedMediaSource when it is the only MSE surface (iPhone)', () => {
  const api = resolveMediaSourceApi({ ManagedMediaSource: Managed });
  assert.equal(api.ctor, Managed);
  assert.equal(api.managed, true);
});

test('ignores a global that cannot probe MIME support', () => {
  const api = resolveMediaSourceApi({ MediaSource: function () {}, ManagedMediaSource: Managed });
  assert.equal(api.ctor, Managed);
});

test('throws when neither API exists', () => {
  assert.throws(() => resolveMediaSourceApi({}), /neither MediaSource nor ManagedMediaSource/);
});
