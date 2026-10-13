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

import { describe, expect, test } from 'vitest'
import { FullTrackName, KeyValuePair, Location, MoqtObject, ObjectForwardingPreference, ObjectStatus } from '@/model'
import { OrderedSubgroupDelivery, OrderedSubgroupStream, type OrderedSubgroupLimits } from './ordered_subgroups'

const track = FullTrackName.tryNew('live/mmtp', 'video')

function object(groupId: bigint, subgroupId: bigint, value: number): MoqtObject {
  return MoqtObject.newWithPayload(
    track,
    new Location(groupId, 0n),
    0,
    ObjectForwardingPreference.Subgroup,
    subgroupId,
    null,
    new Uint8Array([value]),
  )
}

function endOfGroup(groupId: bigint, subgroupId: bigint): MoqtObject {
  return MoqtObject.newWithStatus(
    track,
    new Location(groupId, 1n),
    0,
    ObjectForwardingPreference.Subgroup,
    subgroupId,
    null,
    ObjectStatus.EndOfGroup,
  )
}

function delivery(
  startGroup: bigint | undefined,
  enqueue: (item: MoqtObject) => void,
  limits?: OrderedSubgroupLimits,
): OrderedSubgroupDelivery {
  return new OrderedSubgroupDelivery(startGroup, enqueue, limits)
}

function complete(
  delivery: OrderedSubgroupDelivery,
  registration: ReturnType<OrderedSubgroupDelivery['register']>,
  objects: MoqtObject[],
): boolean {
  for (const item of objects) delivery.append(registration, item)
  return delivery.complete(registration)
}

describe('OrderedSubgroupDelivery', () => {
  test('streams live objects before FIN but holds successors and close events behind FIN', () => {
    const events: string[] = []
    const ordered = delivery(40n, (item) => events.push(`object:${item.payload![0]!}`))
    const first = ordered.register(40n, 0n, false, () => events.push('closed:0'))
    const second = ordered.register(40n, 1n, true, () => events.push('closed:1'))

    ordered.append(first, object(40n, 0n, 1))
    expect(events).toEqual(['object:1'])
    ordered.append(second, object(40n, 1n, 3))
    ordered.append(first, object(40n, 0n, 2))
    expect(events).toEqual(['object:1', 'object:2'])

    ordered.complete(first)
    expect(events).toEqual(['object:1', 'object:2', 'closed:0', 'object:3'])
    ordered.append(second, object(40n, 1n, 4))
    expect(events).toEqual(['object:1', 'object:2', 'closed:0', 'object:3', 'object:4'])
    ordered.complete(second)
    expect(events).toEqual(['object:1', 'object:2', 'closed:0', 'object:3', 'object:4', 'closed:1'])
    expect(() => ordered.finish()).not.toThrow()
  })

  test('does not retain an entire live subgroup against the pending object and byte budgets', () => {
    const emitted: MoqtObject[] = []
    const ordered = delivery(40n, (item) => emitted.push(item), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 1,
      maxPendingBytes: 1,
    })
    const first = ordered.register(40n, 0n, true)
    for (let frame = 0; frame < 30; frame++) {
      const item = object(40n, 0n, frame)
      expect(ordered.append(first, item)).toBe(true)
      expect(emitted.at(-1)).toBe(item)
    }
    expect(emitted).toHaveLength(30)
    expect(ordered.complete(first)).toBe(true)
  })

  test('preserves an empty expected subgroup when a successor fills the object budget', () => {
    const ordered = delivery(40n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 1,
      maxPendingBytes: 10,
    })
    const expected = ordered.register(40n, 0n, true)
    const successor = ordered.register(40n, 1n, true)
    expect(ordered.append(successor, object(40n, 1n, 1))).toBe(true)
    expect(ordered.append(expected, object(40n, 0n, 0))).toBe(true)
    expect(ordered.complete(expected)).toBe(true)
    expect(ordered.append(successor, object(40n, 1n, 2))).toBe(false)
  })

  test('preserves an empty expected subgroup when a successor fills the byte budget', () => {
    const ordered = delivery(40n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 1,
    })
    const expected = ordered.register(40n, 0n, true)
    const successor = ordered.register(40n, 1n, true)
    expect(ordered.append(successor, object(40n, 1n, 1))).toBe(true)
    expect(ordered.append(expected, object(40n, 0n, 0))).toBe(true)
    expect(ordered.complete(expected)).toBe(true)
    expect(ordered.append(successor, object(40n, 1n, 2))).toBe(false)
  })

  test('holds the next group until FIN even after streaming an END_OF_GROUP object', () => {
    const emitted: MoqtObject[] = []
    const ordered = delivery(40n, (item) => emitted.push(item))
    const first = ordered.register(40n, 0n, false)
    const next = ordered.register(41n, 0n, true)
    complete(ordered, next, [object(41n, 0n, 2)])
    ordered.append(first, object(40n, 0n, 1))
    ordered.append(first, endOfGroup(40n, 0n))
    expect(emitted.map((item) => item.groupId)).toEqual([40n, 40n])
    ordered.complete(first)
    expect(emitted.map((item) => item.groupId)).toEqual([40n, 40n, 41n])
  })

  test('retiring during streaming stops the remaining buffered objects and close notification', () => {
    const emitted: number[] = []
    let closed = false
    const ordered = delivery(40n, (item) => {
      emitted.push(item.payload![0]!)
      ordered.retire()
    })
    const first = ordered.register(40n, 0n, false)
    const second = ordered.register(40n, 1n, true, () => {
      closed = true
    })
    complete(ordered, second, [object(40n, 1n, 1), object(40n, 1n, 2)])
    ordered.complete(first)
    expect(emitted).toEqual([1])
    expect(closed).toBe(false)
    expect(ordered.append(second, object(40n, 1n, 3))).toBe(false)
  })

  test('uses coordinates when headers and bodies complete in reverse order', () => {
    const events: string[] = []
    const ordered = delivery(40n, (item) => events.push(`object:${item.payload![0]!}`))

    const second = ordered.register(40n, 1n, true, () => events.push('closed:1'))
    const first = ordered.register(40n, 0n, false, () => events.push('closed:0'))
    complete(ordered, second, [object(40n, 1n, 2)])
    expect(events).toEqual([])

    complete(ordered, first, [object(40n, 0n, 1)])
    expect(events).toEqual(['object:1', 'closed:0', 'object:2', 'closed:1'])
  })

  test('holds a later complete group until the current group reaches END_OF_GROUP', () => {
    const emitted: number[] = []
    const delivery = new OrderedSubgroupDelivery(7n, (item) => emitted.push(item.payload![0]!))

    const next = delivery.register(8n, 0n, true)
    const current = delivery.register(7n, 0n, true)
    complete(delivery, next, [object(8n, 0n, 8)])
    expect(emitted).toEqual([])

    complete(delivery, current, [object(7n, 0n, 7)])
    expect(emitted).toEqual([7, 8])
  })

  test('uses the final END_OF_GROUP object when the stream header was opened without the flag', () => {
    const emitted: MoqtObject[] = []
    const delivery = new OrderedSubgroupDelivery(12n, (item) => emitted.push(item))

    const current = delivery.register(12n, 0n, false)
    const next = delivery.register(13n, 0n, false)
    complete(delivery, next, [object(13n, 0n, 13)])
    complete(delivery, current, [object(12n, 0n, 12), endOfGroup(12n, 0n)])

    expect(emitted.map((item) => item.groupId)).toEqual([12n, 12n, 13n])
  })

  test('rejects END_OF_GROUP before the subgroup final object', () => {
    const delivery = new OrderedSubgroupDelivery(14n, () => {})
    const subgroup = delivery.register(14n, 0n, false)
    delivery.append(subgroup, endOfGroup(14n, 0n))
    expect(() => delivery.append(subgroup, object(14n, 0n, 1))).toThrow(/END_OF_GROUP is not the final object/)
  })

  test('holds a coordinate gap without inferring a terminal subgroup', () => {
    const emitted: number[] = []
    const delivery = new OrderedSubgroupDelivery(4n, (item) => emitted.push(item.payload![0]!))

    const third = delivery.register(4n, 2n, true)
    complete(delivery, third, [object(4n, 2n, 3)])
    expect(emitted).toEqual([])
    delivery.retire()
  })

  test('rejects duplicate and stale coordinates', () => {
    const delivery = new OrderedSubgroupDelivery(2n, () => {})
    const first = delivery.register(2n, 0n, true)
    expect(() => delivery.register(2n, 0n, true)).toThrow(/Duplicate subgroup coordinate/)
    complete(delivery, first, [object(2n, 0n, 1)])
    expect(() => delivery.register(2n, 0n, true)).toThrow(/Stale subgroup coordinate/)
  })

  test('abandons an explicit subgroup from before the live-edge start', () => {
    const emitted: number[] = []
    let abandoned = 0
    const delivery = new OrderedSubgroupDelivery(94n, (item) => emitted.push(item.payload![0]!))

    const previous = delivery.register(93n, 4n, true, undefined, () => abandoned++)
    expect(previous.abandoned).toBe(true)
    expect(delivery.append(previous, object(93n, 4n, 93))).toBe(false)
    expect(delivery.complete(previous)).toBe(false)
    expect(abandoned).toBe(1)

    const live = delivery.register(94n, 0n, true)
    complete(delivery, live, [object(94n, 0n, 94)])
    expect(emitted).toEqual([94])
  })

  test('abandons a reserved subgroup from before the live-edge start', () => {
    const emitted: number[] = []
    let abandoned = 0
    const delivery = new OrderedSubgroupDelivery(94n, (item) => emitted.push(item.payload![0]!))

    const reservation = delivery.reserve(93n, true, () => abandoned++)
    expect(delivery.isReservationActive(reservation)).toBe(false)
    const previous = delivery.registerReserved(reservation, 93n, 4n, true)
    expect(previous.abandoned).toBe(true)
    expect(delivery.append(previous, object(93n, 4n, 93))).toBe(false)
    expect(delivery.complete(previous)).toBe(false)
    expect(abandoned).toBe(1)

    const live = delivery.register(94n, 0n, true)
    complete(delivery, live, [object(94n, 0n, 94)])
    expect(emitted).toEqual([94])
  })

  test('rejects a registered subgroup after an END_OF_GROUP coordinate', () => {
    const delivery = new OrderedSubgroupDelivery(9n, () => {})
    const end = delivery.register(9n, 0n, true)
    delivery.register(9n, 1n, false)
    delivery.append(end, object(9n, 0n, 1))
    expect(() => delivery.complete(end)).toThrow(/follows END_OF_GROUP/)
  })

  test('rejects objects whose native coordinate differs from their stream', () => {
    const delivery = new OrderedSubgroupDelivery(11n, () => {})
    const subgroup = delivery.register(11n, 0n, true)
    expect(() => delivery.append(subgroup, object(11n, 1n, 1))).toThrow(/does not match subgroup/)
  })

  test('ignores a late completion after its delivery generation retires', () => {
    const emitted: number[] = []
    const ordered = delivery(20n, (item) => emitted.push(item.payload![0]!))
    const late = ordered.register(20n, 0n, true)

    ordered.retire()

    expect(ordered.append(late, object(20n, 0n, 1))).toBe(false)
    expect(ordered.complete(late)).toBe(false)
    expect(emitted).toEqual([])
  })

  test('rejects terminal completion while an authored coordinate remains unresolved', () => {
    const ordered = delivery(50n, () => {})
    const successor = ordered.register(50n, 1n, true)
    complete(ordered, successor, [object(50n, 1n, 2)])

    expect(() => ordered.finish()).toThrow(/ended before subgroup 50:0/)
    ordered.retire()
  })

  test('evicts the oldest pending subgroup at the ceiling instead of failing', () => {
    // Regression for Blockcast/pim-multicast-gateway#2028. This used to throw a
    // ProtocolViolationError, and client.ts disconnects on ANY error escaping
    // handleRecvStreams, so hitting a LOCAL window bound killed the session.
    const ordered = delivery(60n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    ordered.register(60n, 1n, false)
    ordered.register(60n, 2n, true)

    expect(() => ordered.register(60n, 3n, true)).not.toThrow()
  })

  test('eviction unblocks head-of-line when the awaited coordinate never arrives', () => {
    // The real #2028 shape: delivery starts mid-stream (joining FETCH not
    // retained), so the coordinate flush waits on is never published and every
    // later subgroup queues behind it forever. Eviction must advance past it so
    // subsequent subgroups can actually be released.
    const released: bigint[] = []
    const ordered = delivery(70n, (object) => released.push(object.location.group), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 64,
      maxPendingBytes: 4096,
    })

    // 70:0 is what flush awaits, and it never arrives. Fill the window past it.
    ordered.register(70n, 1n, false)
    ordered.register(70n, 2n, false)
    expect(released).toHaveLength(0)

    // Exceeding the window must make progress, not throw.
    expect(() => ordered.register(70n, 3n, false)).not.toThrow()
  })

  test('keeps a late awaited coordinate ahead of a full later window', () => {
    const emitted: number[] = []
    const ordered = delivery(70n, (item) => emitted.push(item.payload![0]!), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 64,
      maxPendingBytes: 4096,
    })
    const second = ordered.register(70n, 1n, false)
    const third = ordered.register(70n, 2n, true)
    complete(ordered, second, [object(70n, 1n, 1)])
    complete(ordered, third, [object(70n, 2n, 2)])

    const first = ordered.register(70n, 0n, false)
    complete(ordered, first, [object(70n, 0n, 0)])

    expect(emitted).toEqual([0, 1])
  })

  test('keeps an awaited reservation when a later coordinate exceeds capacity', () => {
    const emitted: number[] = []
    const ordered = delivery(70n, (item) => emitted.push(item.payload![0]!), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 64,
      maxPendingBytes: 4096,
    })
    const firstReservation = ordered.reserve(70n)
    const second = ordered.register(70n, 1n, true)
    complete(ordered, second, [object(70n, 1n, 1)])

    const discarded = ordered.register(70n, 2n, true)
    expect(ordered.append(discarded, object(70n, 2n, 2))).toBe(false)
    const first = ordered.registerReserved(firstReservation, 70n, 0n, false)
    complete(ordered, first, [object(70n, 0n, 0)])

    expect(emitted).toEqual([0, 1])
  })

  test('evicting an END_OF_GROUP subgroup advances to the next group, not a nonexistent subgroup', () => {
    // Eviction must reuse flush's END_OF_GROUP transition. Resuming at
    // (groupId, subgroupId + 1) after dropping a subgroup that ends its group
    // leaves flush awaiting a subgroup that will never be published, so every
    // later group stays blocked and playback still emits zero frames.
    const released: bigint[] = []
    const ordered = delivery(80n, (object) => released.push(object.location.group), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 64,
      maxPendingBytes: 4096,
    })

    // 80:0 ends group 80 but is held incomplete, so it stays pending while the
    // window fills.
    const ending = ordered.register(80n, 0n, true)
    ordered.append(ending, endOfGroup(80n, 0n))
    ordered.register(80n, 1n, false)

    // Overflow evicts 80:0. Because it ends its group, the next valid
    // coordinate is 81:0 — not the nonexistent 80:1-after-END_OF_GROUP.
    const firstOf81 = ordered.register(81n, 0n, false)
    complete(ordered, firstOf81, [object(81n, 0n, 8)])

    // The later group is only deliverable if the expectation advanced to 81:0.
    expect(released).toContain(81n)
  })

  test('an evicted subgroup whose stream is still draining does not fail delivery', () => {
    // Eviction drops the map entry while the transport reader keeps running to
    // FIN. Its remaining append()/complete() calls must be non-fatal: throwing
    // ProtocolViolationError here is converted to a full client disconnect,
    // which is the exact failure the eviction exists to prevent (#2028).
    const ordered = delivery(90n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 64,
      maxPendingBytes: 4096,
    })

    const draining = ordered.register(90n, 0n, false)
    ordered.register(90n, 1n, false)

    // Overflow evicts 90:0 while its stream is still in flight.
    ordered.register(90n, 2n, false)

    // The reader's remaining calls report non-delivery instead of throwing.
    expect(ordered.append(draining, object(90n, 0n, 1))).toBe(false)
    let completed: boolean | undefined
    expect(() => {
      completed = ordered.complete(draining)
    }).not.toThrow()
    expect(completed).toBe(false)
  })

  test('does not report a completed subgroup whose buffered objects were evicted', () => {
    let released = 0
    const ordered = delivery(99n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 64,
      maxPendingBytes: 4096,
    })
    const dropped = ordered.register(100n, 0n, true, () => released++)
    complete(ordered, dropped, [object(100n, 0n, 1)])
    ordered.register(101n, 0n, true)

    ordered.register(102n, 0n, true)

    expect(released).toBe(0)
  })

  test('evicts first-object-ID reservations without failing delivery at the ceiling', () => {
    const abandoned: number[] = []
    const ordered = delivery(63n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    ordered.reserve(63n, false, () => abandoned.push(1))
    ordered.reserve(63n, false, () => abandoned.push(2))

    expect(() => ordered.reserve(63n, false, () => abandoned.push(3))).not.toThrow()
    expect(abandoned).toEqual([1])
    expect(() => ordered.register(63n, 0n, true)).not.toThrow()
    expect(abandoned).toEqual([1, 2])
  })

  test('reservation eviction releases first-object-ID promotion after remaining identities arrive', () => {
    const released: bigint[] = []
    const ordered = delivery(63n, (item) => released.push(item.subgroupId!), {
      maxPendingSubgroups: 3,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    const abandoned = ordered.reserve(63n)
    const first = ordered.reserve(63n)
    const second = ordered.reserve(63n)
    const third = ordered.reserve(63n)

    const abandonedRegistration = ordered.registerReserved(abandoned, 63n, 0n, false)
    expect(ordered.append(abandonedRegistration, object(63n, 0n, 0))).toBe(false)
    const firstRegistration = ordered.registerReserved(first, 63n, 32n, false)
    complete(ordered, firstRegistration, [object(63n, 32n, 1)])
    const secondRegistration = ordered.registerReserved(second, 63n, 64n, false)
    complete(ordered, secondRegistration, [object(63n, 64n, 2)])
    const thirdRegistration = ordered.registerReserved(third, 63n, 96n, true)
    complete(ordered, thirdRegistration, [object(63n, 96n, 3)])

    expect(released).toEqual([32n, 64n, 96n])
    expect(() => ordered.finish()).not.toThrow()
  })

  test('evicting an unidentified END_OF_GROUP reservation advances to the next group', () => {
    const released: bigint[] = []
    const ordered = delivery(80n, (item) => released.push(item.location.group), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    ordered.reserve(80n, true)
    const group81 = ordered.reserve(81n, true)
    const group82 = ordered.reserve(82n, true)

    const first = ordered.registerReserved(group81, 81n, 0n, true)
    complete(ordered, first, [object(81n, 0n, 1)])
    const second = ordered.registerReserved(group82, 82n, 0n, true)
    complete(ordered, second, [object(82n, 0n, 2)])

    expect(released).toEqual([81n, 82n])
  })

  test('does not advance past a same-group reservation when an unidentified terminal stream is evicted', () => {
    const released: bigint[] = []
    const ordered = delivery(70n, (item) => released.push(item.subgroupId!), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    const terminal = ordered.reserve(70n, true)
    const head = ordered.reserve(70n, false)
    const tail = ordered.reserve(70n, false)

    const abandonedTerminal = ordered.registerReserved(terminal, 70n, 32n, true)
    expect(ordered.append(abandonedTerminal, object(70n, 32n, 32))).toBe(false)
    const first = ordered.registerReserved(head, 70n, 0n, false)
    complete(ordered, first, [object(70n, 0n, 0)])
    const last = ordered.registerReserved(tail, 70n, 64n, false)
    complete(ordered, last, [object(70n, 64n, 64)])
    const next = ordered.register(71n, 0n, true)
    complete(ordered, next, [object(71n, 0n, 71)])

    expect(released).toEqual([0n, 64n, 0n])
  })

  test('holds deferred reservation END_OF_GROUP advancement behind the header barrier', () => {
    const released: bigint[] = []
    let headersResolved = true
    const ordered = new OrderedSubgroupDelivery(
      70n,
      (item) => released.push(item.location.group),
      { maxPendingSubgroups: 2, maxPendingObjects: 10, maxPendingBytes: 10 },
      () => headersResolved,
    )
    const terminal = ordered.reserve(70n, true)
    const head = ordered.reserve(70n, false)
    const tail = ordered.reserve(70n, false)
    expect(ordered.registerReserved(terminal, 70n, 32n, true).abandoned).toBe(true)
    const first = ordered.registerReserved(head, 70n, 0n, false)
    const last = ordered.registerReserved(tail, 70n, 64n, false)
    complete(ordered, first, [object(70n, 0n, 0)])

    headersResolved = false
    complete(ordered, last, [object(70n, 64n, 64)])
    const next = ordered.register(71n, 0n, true)
    complete(ordered, next, [object(71n, 0n, 71)])
    expect(released).toEqual([70n, 70n])

    headersResolved = true
    ordered.resume()
    expect(released).toEqual([70n, 70n, 71n])
  })

  test('evicts an unidentified reservation before known pending media', () => {
    let reservationAbandoned = 0
    let pendingAbandoned = 0
    const ordered = delivery(63n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    ordered.reserve(63n, false, () => reservationAbandoned++)
    const pending = ordered.register(64n, 0n, true, undefined, () => pendingAbandoned++)
    ordered.append(pending, object(64n, 0n, 1))

    expect(() => ordered.register(65n, 0n, true)).not.toThrow()
    expect(reservationAbandoned).toBe(1)
    expect(pendingAbandoned).toBe(0)
  })

  test('names the abandon reason so a pre-window drop is not read as window pressure', () => {
    const reasons: string[] = []

    // Outside the delivery window: a publisher stream already in flight for an
    // earlier group. Expected, and nothing to do with this receiver's limits.
    const beforeWindow = delivery(20n, () => {})
    beforeWindow.register(19n, 0n, true, undefined, (reason) => reasons.push(reason))
    beforeWindow.reserve(18n, false, (reason) => reasons.push(reason))
    expect(reasons).toEqual(['before-delivery-window', 'before-delivery-window'])

    // Receiver-side pressure, at BOTH eviction sites. The reason is a required
    // parameter at the reservation site and hardcoded at the pending site, so
    // each is pinned separately: a wrong literal in #discardPending is
    // otherwise invisible to this suite.
    reasons.length = 0
    const pressured = delivery(63n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    pressured.reserve(63n, false, (reason) => reasons.push(reason))
    const pending = pressured.register(64n, 0n, true, undefined, (reason) => reasons.push(reason))
    pressured.append(pending, object(64n, 0n, 1))
    // Over the subgroup ceiling: the unidentified reservation is dropped first,
    // because it holds no media and precedes what does (#discardReservation).
    pressured.register(65n, 0n, true)
    // Over it again with no reservation left to give up, so the oldest pending
    // coordinate goes — and that one is carrying the appended object, which is
    // what makes this a dropped-media eviction rather than a dropped
    // placeholder (#discardPending).
    pressured.register(66n, 0n, true)
    expect(reasons).toEqual(['window-pressure', 'window-pressure'])
  })

  test('does not skip known media to evict an unidentified future group', () => {
    let reservationAbandoned = 0
    let pendingAbandoned = 0
    const ordered = delivery(10n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    const pending = ordered.register(10n, 1n, false, undefined, () => pendingAbandoned++)
    ordered.append(pending, object(10n, 1n, 1))
    ordered.reserve(100n, false, () => reservationAbandoned++)

    ordered.register(101n, 0n, true)

    expect(pendingAbandoned).toBe(1)
    expect(reservationAbandoned).toBe(0)
  })

  test('converts a reservation without double-counting subgroup capacity', () => {
    const ordered = delivery(64n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 10,
    })
    const reservation = ordered.reserve()
    ordered.register(64n, 1n, true)

    expect(() => ordered.registerReserved(reservation, 64n, 0n, false)).not.toThrow()
    // Capacity is still accounted correctly; the window now makes room by
    // evicting the oldest pending coordinate rather than throwing (#2028).
    expect(() => ordered.reserve()).not.toThrow()
  })

  test('rejects late reserved registration after generation retirement', () => {
    const ordered = delivery(65n, () => {})
    const reservation = ordered.reserve()
    ordered.retire()

    expect(() => ordered.registerReserved(reservation, 65n, 0n, true)).toThrow(/generation is retired/)
  })

  test('rejects a reservation owned by another delivery generation', () => {
    const first = delivery(66n, () => {})
    const second = delivery(66n, () => {})
    const reservation = first.reserve()

    expect(() => second.registerReserved(reservation, 66n, 0n, true)).toThrow(/Reservation is not active/)
  })

  test('rejects terminal completion while a subgroup still awaits identity', () => {
    const ordered = delivery(67n, () => {})
    ordered.reserve()

    expect(() => ordered.finish()).toThrow(/1 subgroup stream\(s\) awaiting identity/)
  })

  test('abandons a subgroup nonfatally at the pending object ceiling', () => {
    let abandoned = 0
    const released: number[] = []
    const ordered = delivery(61n, (item) => released.push(item.payload![0]!), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 2,
      maxPendingBytes: 10,
    })
    const successor = ordered.register(61n, 1n, false, undefined, () => abandoned++)
    ordered.append(successor, object(61n, 1n, 1))
    ordered.append(successor, object(61n, 1n, 2))

    expect(ordered.append(successor, object(61n, 1n, 3))).toBe(false)
    expect(abandoned).toBe(1)
    const next = ordered.register(61n, 2n, true)
    complete(ordered, next, [object(61n, 2n, 4)])
    expect(released).toEqual([4])
  })

  test('abandons a subgroup nonfatally at the pending byte ceiling', () => {
    let abandoned = 0
    const ordered = delivery(62n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: 2,
    })
    const successor = ordered.register(62n, 1n, true, undefined, () => abandoned++)
    ordered.append(successor, object(62n, 1n, 1))
    ordered.append(successor, object(62n, 1n, 2))

    expect(ordered.append(successor, object(62n, 1n, 3))).toBe(false)
    expect(abandoned).toBe(1)
  })

  test('charges retained extension headers against the pending byte ceiling', () => {
    const extension = KeyValuePair.tryNewBytes(1n, new Uint8Array(8))
    const retainedBytes = extension.serialize().length
    const ordered = delivery(68n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 10,
      maxPendingBytes: retainedBytes - 1,
    })
    const successor = ordered.register(68n, 1n, true)
    const extensionHeavyObject = MoqtObject.newWithPayload(
      track,
      new Location(68n, 0n),
      0,
      ObjectForwardingPreference.Subgroup,
      1n,
      [extension],
      new Uint8Array(),
    )

    expect(ordered.append(successor, extensionHeavyObject)).toBe(false)
  })

  test('registers first-object-ID mode from the first object before release', () => {
    const emitted: number[] = []
    const ordered = delivery(70n, (item) => emitted.push(item.payload![0]!))
    const stream = new OrderedSubgroupStream(70n, null, true, true, (groupId, subgroupId, containsEnd) => {
      const registration = ordered.register(groupId, subgroupId, containsEnd)
      return {
        append: (item) => ordered.append(registration, item),
        complete: () => ordered.complete(registration),
      }
    })

    expect(stream.append(object(70n, 0n, 7))).toBe(true)
    expect(stream.complete()).toBe(true)
    expect(emitted).toEqual([7])
  })

  test('releases non-contiguous first-object-ID subgroups in authored order', () => {
    const emitted: number[] = []
    const ordered = delivery(72n, (item) => emitted.push(item.payload![0]!))
    const firstReservation = ordered.reserve()
    const secondReservation = ordered.reserve()
    const first = ordered.registerReserved(firstReservation, 72n, 0n, false)
    const second = ordered.registerReserved(secondReservation, 72n, 32n, true)

    complete(ordered, second, [object(72n, 32n, 2)])
    expect(emitted).toEqual([])

    complete(ordered, first, [object(72n, 0n, 1)])
    expect(emitted).toEqual([1, 2])
  })

  test('does not promote an identified successor ahead of an unresolved reservation', () => {
    const emitted: number[] = []
    const ordered = delivery(74n, (item) => emitted.push(item.payload![0]!))
    const firstReservation = ordered.reserve()
    const secondReservation = ordered.reserve()
    const thirdReservation = ordered.reserve()
    const first = ordered.registerReserved(firstReservation, 74n, 0n, false)

    complete(ordered, first, [object(74n, 0n, 0)])
    const third = ordered.registerReserved(thirdReservation, 74n, 64n, true)
    complete(ordered, third, [object(74n, 64n, 64)])
    expect(emitted).toEqual([0])

    const second = ordered.registerReserved(secondReservation, 74n, 32n, false)
    complete(ordered, second, [object(74n, 32n, 32)])
    expect(emitted).toEqual([0, 32, 64])
  })

  test('does not let a future-group reservation block ready current-group media', () => {
    const emitted: number[] = []
    const ordered = delivery(10n, (item) => emitted.push(item.payload![0]!))
    const firstReservation = ordered.reserve(10n)
    const lastReservation = ordered.reserve(10n)
    const first = ordered.registerReserved(firstReservation, 10n, 0n, false)
    const last = ordered.registerReserved(lastReservation, 10n, 32n, true)
    ordered.reserve(11n)

    complete(ordered, last, [object(10n, 32n, 32)])
    complete(ordered, first, [object(10n, 0n, 0)])

    expect(emitted).toEqual([0, 32])
  })

  test('does not promote a successor while an earlier transport header is unresolved', () => {
    const emitted: number[] = []
    let headersResolved = true
    const ordered = new OrderedSubgroupDelivery(
      75n,
      (item) => emitted.push(item.payload![0]!),
      undefined,
      () => headersResolved,
    )
    const firstReservation = ordered.reserve()
    const first = ordered.registerReserved(firstReservation, 75n, 0n, false)
    complete(ordered, first, [object(75n, 0n, 0)])

    headersResolved = false
    const thirdReservation = ordered.reserve()
    const third = ordered.registerReserved(thirdReservation, 75n, 64n, true)
    complete(ordered, third, [object(75n, 64n, 64)])
    expect(emitted).toEqual([0])

    const secondReservation = ordered.reserve()
    const second = ordered.registerReserved(secondReservation, 75n, 32n, false)
    complete(ordered, second, [object(75n, 32n, 32)])
    expect(emitted).toEqual([0])

    headersResolved = true
    ordered.resume()
    expect(emitted).toEqual([0, 32, 64])
  })

  test('rejects terminal completion while awaiting the next first-object-ID subgroup', () => {
    const ordered = delivery(73n, () => {})
    const reservation = ordered.reserve()
    const first = ordered.registerReserved(reservation, 73n, 0n, false)
    complete(ordered, first, [object(73n, 0n, 1)])

    expect(() => ordered.finish()).toThrow(/ended before subgroup 73:next authored subgroup/)
  })

  test('rejects an empty first-object-ID subgroup stream', () => {
    const ordered = delivery(71n, () => {})
    const stream = new OrderedSubgroupStream(71n, null, true, true, (groupId, subgroupId, containsEnd) => {
      const registration = ordered.register(groupId, subgroupId, containsEnd)
      return {
        append: (item) => ordered.append(registration, item),
        complete: () => ordered.complete(registration),
      }
    })

    expect(() => stream.complete()).toThrow(/ended before its first object/)
  })

  test('the awaited coordinate arriving late at a full window is not stranded', () => {
    const released: bigint[] = []
    const ordered = delivery(70n, (object) => released.push(object.subgroupId ?? -1n), {
      maxPendingSubgroups: 2,
      maxPendingObjects: 64,
      maxPendingBytes: 4096,
    })

    const one = ordered.register(70n, 1n, false)
    complete(ordered, one, [object(70n, 1n, 1)])
    const two = ordered.register(70n, 2n, false)
    complete(ordered, two, [object(70n, 2n, 2)])
    expect(released).toHaveLength(0)

    const zero = ordered.register(70n, 0n, false)
    complete(ordered, zero, [object(70n, 0n, 0)])

    expect(released[0]).toBe(0n)
  })

  test('eviction does not strand a coordinate owned by an unresolved reservation', () => {
    const ordered = delivery(70n, () => {}, {
      maxPendingSubgroups: 2,
      maxPendingObjects: 64,
      maxPendingBytes: 4096,
    })

    const reservation = ordered.reserve()
    ordered.register(70n, 1n, false)
    ordered.register(70n, 2n, false)

    expect(() => ordered.registerReserved(reservation, 70n, 0n, false)).not.toThrow()
  })

  describe('without a publisher-signaled start group', () => {
    test('anchors on the first registered group and releases it', () => {
      // SUBSCRIBE_OK without Largest Location used to anchor the window at group 0,
      // so a live stream whose first delivered group was 38 held every group until
      // the pending window overflowed (Blockcast/hang-mmt-fec#520 is the publisher
      // side; this is the receiver side).
      const emitted: number[] = []
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))

      const first = ordered.register(38n, 0n, true)
      complete(ordered, first, [object(38n, 0n, 38)])
      expect(emitted).toEqual([38])

      const next = ordered.register(39n, 0n, true)
      complete(ordered, next, [object(39n, 0n, 39)])
      expect(emitted).toEqual([38, 39])
      expect(() => ordered.finish()).not.toThrow()
    })

    test('holds later groups behind the anchoring group until it ends', () => {
      const emitted: number[] = []
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))

      const anchor = ordered.register(38n, 0n, true)
      const later = ordered.register(39n, 0n, true)
      complete(ordered, later, [object(39n, 0n, 39)])
      expect(emitted).toEqual([])

      complete(ordered, anchor, [object(38n, 0n, 38)])
      expect(emitted).toEqual([38, 39])
    })

    test('re-anchors on a lower group that arrives before anything is released', () => {
      const emitted: number[] = []
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))

      // Group 39's header arrived first, but no media has been delivered yet.
      const later = ordered.register(39n, 0n, true)
      const earlier = ordered.register(38n, 0n, true)
      expect(earlier.abandoned).toBe(false)

      ordered.append(later, object(39n, 0n, 39))
      expect(ordered.complete(later)).toBe(true)
      expect(emitted).toEqual([])

      complete(ordered, earlier, [object(38n, 0n, 38)])
      expect(emitted).toEqual([38, 39])
    })

    test('re-anchors repeatedly while nothing has been released', () => {
      const emitted: number[] = []
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))

      const third = ordered.register(40n, 0n, true)
      const second = ordered.register(39n, 0n, true)
      const first = ordered.register(38n, 0n, true)
      complete(ordered, third, [object(40n, 0n, 40)])
      complete(ordered, second, [object(39n, 0n, 39)])
      expect(emitted).toEqual([])

      complete(ordered, first, [object(38n, 0n, 38)])
      expect(emitted).toEqual([38, 39, 40])
    })

    test('does not re-anchor below a group skipped by eviction before release', () => {
      const ordered = delivery(undefined, () => {}, {
        maxPendingSubgroups: 2,
        maxPendingObjects: 64,
        maxPendingBytes: 4096,
      })

      // Anchor at 40, then force eviction of that anchor before its stream can
      // release. The cursor has committed to the next group even though no
      // object has reached the consumer yet.
      const anchor = ordered.register(40n, 0n, true)
      ordered.register(41n, 0n, false)
      ordered.register(42n, 0n, false)

      const earlier = ordered.register(39n, 0n, true)
      expect(earlier.abandoned).toBe(true)
    })

    test('treats lower coordinates as stale once anything has been released', () => {
      const emitted: number[] = []
      let abandoned = 0
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))

      const first = ordered.register(38n, 0n, true)
      complete(ordered, first, [object(38n, 0n, 38)])
      const second = ordered.register(39n, 0n, true)
      complete(ordered, second, [object(39n, 0n, 39)])
      expect(emitted).toEqual([38, 39])

      // Below the anchor: a pre-window race, abandoned like an explicit start.
      const before = ordered.register(37n, 0n, true, undefined, () => abandoned++)
      expect(before.abandoned).toBe(true)
      expect(ordered.append(before, object(37n, 0n, 37))).toBe(false)
      expect(abandoned).toBe(1)

      // Inside the window but behind the expectation: a protocol violation.
      expect(() => ordered.register(38n, 1n, false)).toThrow(/Stale subgroup coordinate 38:1/)
      expect(emitted).toEqual([38, 39])
    })

    test('fixes the live anchor on the first streamed object, before subgroup FIN', () => {
      const emitted: number[] = []
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))
      const first = ordered.register(38n, 0n, true)
      ordered.append(first, object(38n, 0n, 38))
      expect(emitted).toEqual([38])
      const earlier = ordered.register(37n, 0n, true)
      expect(earlier.abandoned).toBe(true)
      expect(ordered.complete(first)).toBe(true)
    })

    test('anchors on a reservation resolved by its first object', () => {
      const emitted: number[] = []
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))

      const reservation = ordered.reserve(38n, true)
      const first = ordered.registerReserved(reservation, 38n, 0n, true)
      expect(first.abandoned).toBe(false)
      complete(ordered, first, [object(38n, 0n, 38)])
      expect(emitted).toEqual([38])

      const next = ordered.register(39n, 0n, true)
      complete(ordered, next, [object(39n, 0n, 39)])
      expect(emitted).toEqual([38, 39])
    })

    test('re-anchors on a lower reserved group resolved before anything is released', () => {
      const emitted: number[] = []
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))

      const reservation = ordered.reserve(38n, true)
      const later = ordered.register(39n, 0n, true)
      const earlier = ordered.registerReserved(reservation, 38n, 0n, true)
      expect(earlier.abandoned).toBe(false)

      complete(ordered, later, [object(39n, 0n, 39)])
      expect(emitted).toEqual([])
      complete(ordered, earlier, [object(38n, 0n, 38)])
      expect(emitted).toEqual([38, 39])
    })

    test('abandons a lower reserved group resolved after the first release', () => {
      const emitted: number[] = []
      let abandoned = 0
      const ordered = delivery(undefined, (item) => emitted.push(item.payload![0]!))

      const reservation = ordered.reserve(38n, true, () => abandoned++)
      const later = ordered.register(39n, 0n, true)
      complete(ordered, later, [object(39n, 0n, 39)])
      expect(emitted).toEqual([39])

      const earlier = ordered.registerReserved(reservation, 38n, 0n, true)
      expect(earlier.abandoned).toBe(true)
      expect(abandoned).toBe(1)
      expect(ordered.isReservationActive(reservation)).toBe(false)
      expect(ordered.append(earlier, object(38n, 0n, 38))).toBe(false)
      expect(ordered.complete(earlier)).toBe(false)
      expect(emitted).toEqual([39])
    })

    test('rejects a reservation without a group while unanchored', () => {
      const ordered = delivery(undefined, () => {})
      expect(() => ordered.reserve()).toThrow(/requires an explicit group while the delivery window is unanchored/)
    })

    test('evicts surplus reservations at the ceiling before any coordinate anchors the window', () => {
      const abandoned: number[] = []
      const ordered = delivery(undefined, () => {}, {
        maxPendingSubgroups: 1,
        maxPendingObjects: 10,
        maxPendingBytes: 10,
      })
      ordered.reserve(38n, false, () => abandoned.push(38))
      expect(() => ordered.reserve(39n, false, () => abandoned.push(39))).not.toThrow()
      expect(abandoned).toEqual([38])
      expect(() => ordered.resume()).not.toThrow()
    })

    test('finishes cleanly when no stream ever registered a coordinate', () => {
      const ordered = delivery(undefined, () => {})
      expect(() => ordered.finish()).not.toThrow()
    })

    test('rejects terminal completion while a reservation still awaits identity', () => {
      const ordered = delivery(undefined, () => {})
      ordered.reserve(38n)
      expect(() => ordered.finish()).toThrow(/1 subgroup stream\(s\) awaiting identity/)
    })
  })
})
