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

import { MoqtObject, ProtocolViolationError } from '@/model'

/**
 * Why a subgroup was abandoned. These are different faults with different
 * owners and must stay distinguishable in logs: `before-delivery-window` is
 * expected behaviour (a publisher stream racing ahead of, or behind, the
 * window anchor), while `window-pressure` means this receiver's own bounded
 * window overflowed and discarded media to make forward progress. Reporting
 * both as "eviction" makes a healthy receiver look like a starving one.
 */
export type OrderedSubgroupAbandonReason = 'before-delivery-window' | 'window-pressure'

type AbandonCallback = (reason: OrderedSubgroupAbandonReason) => void

type PendingSubgroup = {
  groupId: bigint
  subgroupId: bigint
  derivesFromFirstObject: boolean
  containsEndOfGroup: boolean
  sawEndOfGroup: boolean
  completed: boolean
  registration: OrderedSubgroupRegistration
  objects: MoqtObject[]
  bytes: number
  onReleased: (() => void) | undefined
  onAbandoned: AbandonCallback | undefined
}

type OrderedSubgroupRegistration = {
  readonly groupId: bigint
  readonly subgroupId: bigint
  readonly containsEndOfGroup: boolean
  abandoned: boolean
}

export type OrderedSubgroupReservation = Readonly<{ token: symbol }>

type OrderedSubgroupReservationState = {
  groupId: bigint
  containsEndOfGroup: boolean
  onAbandoned: AbandonCallback | undefined
}

function coordinateKey(groupId: bigint, subgroupId: bigint): string {
  return `${groupId}:${subgroupId}`
}

function retainedObjectBytes(object: MoqtObject): number {
  let bytes = object.payload?.byteLength ?? 0
  for (const extension of object.properties ?? []) bytes += extension.serialize().length
  return bytes
}

export type OrderedSubgroupLimits = Readonly<{
  maxPendingSubgroups: number
  maxPendingObjects: number
  maxPendingBytes: number
}>

export type OrderedSubgroupReceiver = Readonly<{
  append: (object: MoqtObject) => boolean
  complete: () => boolean
}>

export class OrderedSubgroupStream {
  readonly #groupId: bigint
  readonly #containsEndOfGroup: boolean
  readonly #deriveFromFirstObject: boolean
  readonly #register: (
    groupId: bigint,
    subgroupId: bigint,
    containsEndOfGroup: boolean,
    onReleased?: () => void,
    onAbandoned?: AbandonCallback,
  ) => OrderedSubgroupReceiver
  readonly #onReleased: (() => void) | undefined
  readonly #onAbandoned: AbandonCallback | undefined
  #receiver: OrderedSubgroupReceiver | undefined

  constructor(
    groupId: bigint,
    subgroupId: bigint | null,
    containsEndOfGroup: boolean,
    deriveFromFirstObject: boolean,
    register: (
      groupId: bigint,
      subgroupId: bigint,
      containsEndOfGroup: boolean,
      onReleased?: () => void,
      onAbandoned?: AbandonCallback,
    ) => OrderedSubgroupReceiver,
    onReleased?: () => void,
    onAbandoned?: AbandonCallback,
  ) {
    this.#groupId = groupId
    this.#containsEndOfGroup = containsEndOfGroup
    this.#deriveFromFirstObject = deriveFromFirstObject
    this.#register = register
    this.#onReleased = onReleased
    this.#onAbandoned = onAbandoned
    if (!deriveFromFirstObject) {
      if (subgroupId === null) {
        throw new ProtocolViolationError(
          'OrderedSubgroupStream',
          `Reliable ordered delivery requires an explicit subgroup ID for group ${groupId}`,
        )
      }
      this.#receiver = register(groupId, subgroupId, containsEndOfGroup, onReleased, onAbandoned)
    }
  }

  append(object: MoqtObject): boolean {
    if (!this.#receiver) {
      if (!this.#deriveFromFirstObject || object.subgroupId === null) {
        throw new ProtocolViolationError(
          'OrderedSubgroupStream',
          `Reliable ordered delivery could not derive a subgroup ID for group ${this.#groupId}`,
        )
      }
      this.#receiver = this.#register(
        this.#groupId,
        object.subgroupId,
        this.#containsEndOfGroup,
        this.#onReleased,
        this.#onAbandoned,
      )
    }
    return this.#receiver.append(object)
  }

  complete(): boolean {
    if (!this.#receiver) {
      throw new ProtocolViolationError(
        'OrderedSubgroupStream',
        `Reliable ordered subgroup ${this.#groupId} ended before its first object`,
      )
    }
    return this.#receiver.complete()
  }
}

export const DEFAULT_ORDERED_SUBGROUP_LIMITS: OrderedSubgroupLimits = Object.freeze({
  maxPendingSubgroups: 256,
  maxPendingObjects: 65_536,
  maxPendingBytes: 64 * 1024 * 1024,
})

function validateLimits(limits: OrderedSubgroupLimits): void {
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new RangeError(`${name} must be a positive safe integer`)
    }
  }
}

/**
 * Streams the current reliable subgroup in publisher-authored coordinate order.
 * Later subgroups wait for its FIN; media within it does not. Transport streams
 * may be accepted and drained in any order; only the explicit `(group, subgroup)`
 * coordinate and END_OF_GROUP flag advance state.
 *
 * The delivery window starts at `startGroup` when the publisher signaled the
 * live edge (SUBSCRIBE_OK carried a Largest Location; the window starts at the
 * following group). Without that signal the window is constructed unanchored
 * and anchors on the first coordinate a stream registers. Transport streams for
 * adjacent groups can be accepted out of order, so until the first subgroup is
 * released a lower group re-anchors the window down to itself instead of being
 * treated as stale. Once anything has been released the anchor is fixed:
 * coordinates below `startGroup` are abandoned as pre-window races and
 * coordinates below the expectation are protocol violations, exactly as for a
 * publisher-signaled start.
 */
export class OrderedSubgroupDelivery {
  readonly #enqueue: (object: MoqtObject) => void
  readonly #limits: OrderedSubgroupLimits
  readonly #canPromote: () => boolean
  readonly #pending = new Map<string, PendingSubgroup>()
  /** True when the constructor received no start group and the anchor comes from delivery. */
  readonly #anchorsOnArrival: boolean
  /** Undefined only while unanchored. `#expectedGroup` is defined exactly when this is. */
  #startGroup: bigint | undefined
  #expectedGroup: bigint | undefined
  #expectedSubgroup: bigint | undefined
  #lastReleasedSubgroup: bigint | undefined
  #deferredEndOfExpectedGroup: bigint | undefined
  /** Set once any subgroup has entered the ordered consumer stream. */
  #hasReleased = false
  #active = true
  readonly #reservations = new Map<OrderedSubgroupReservation, OrderedSubgroupReservationState>()
  readonly #abandonedReservations = new WeakSet<OrderedSubgroupReservation>()
  #pendingObjects = 0
  #pendingBytes = 0

  constructor(
    startGroup: bigint | undefined,
    enqueue: (object: MoqtObject) => void,
    limits: OrderedSubgroupLimits = DEFAULT_ORDERED_SUBGROUP_LIMITS,
    canPromote: () => boolean = () => true,
  ) {
    validateLimits(limits)
    this.#anchorsOnArrival = startGroup === undefined
    this.#startGroup = startGroup
    this.#expectedGroup = startGroup
    this.#expectedSubgroup = startGroup === undefined ? undefined : 0n
    this.#enqueue = enqueue
    this.#limits = limits
    this.#canPromote = canPromote
  }

  register(
    groupId: bigint,
    subgroupId: bigint,
    containsEndOfGroup: boolean,
    onReleased?: () => void,
    onAbandoned?: AbandonCallback,
  ): OrderedSubgroupRegistration {
    if (!this.#active) {
      throw new ProtocolViolationError('OrderedSubgroupDelivery.register', 'Delivery generation is retired')
    }
    const expectedGroup = this.#admit(groupId)
    if (expectedGroup === undefined) {
      onAbandoned?.('before-delivery-window')
      return { groupId, subgroupId, containsEndOfGroup, abandoned: true }
    }
    if (this.#isStale(expectedGroup, groupId, subgroupId)) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.register',
        `Stale subgroup coordinate ${groupId}:${subgroupId}; expected ${expectedGroup}:${this.#expectedSubgroup ?? 'next authored subgroup'}`,
      )
    }

    const key = coordinateKey(groupId, subgroupId)
    if (this.#pending.has(key)) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.register',
        `Duplicate subgroup coordinate ${groupId}:${subgroupId}`,
      )
    }
    const registration: OrderedSubgroupRegistration = {
      groupId,
      subgroupId,
      containsEndOfGroup,
      abandoned: false,
    }
    this.#pending.set(key, {
      groupId,
      subgroupId,
      derivesFromFirstObject: false,
      containsEndOfGroup,
      sawEndOfGroup: false,
      completed: false,
      registration,
      objects: [],
      bytes: 0,
      onReleased,
      onAbandoned,
    })
    this.resume()
    this.#enforceSubgroupLimit({ key, groupId, subgroupId })
    return registration
  }

  reserve(groupId?: bigint, containsEndOfGroup = false, onAbandoned?: AbandonCallback): OrderedSubgroupReservation {
    if (!this.#active) {
      throw new ProtocolViolationError('OrderedSubgroupDelivery.reserve', 'Delivery generation is retired')
    }
    const reservedGroup = groupId ?? this.#expectedGroup
    if (reservedGroup === undefined) {
      throw new TypeError(
        'OrderedSubgroupDelivery.reserve requires an explicit group while the delivery window is unanchored',
      )
    }
    const reservation = Object.freeze({ token: Symbol('ordered subgroup reservation') })
    const state = { groupId: reservedGroup, containsEndOfGroup, onAbandoned }
    this.#reservations.set(reservation, state)
    if (this.#precedesWindow(reservedGroup)) {
      this.#discardReservation(reservation, state, false, 'before-delivery-window')
      return reservation
    }
    this.#enforceSubgroupLimit()
    return reservation
  }

  isReservationActive(reservation: OrderedSubgroupReservation): boolean {
    return this.#reservations.has(reservation)
  }

  registerReserved(
    reservation: OrderedSubgroupReservation,
    groupId: bigint,
    subgroupId: bigint,
    containsEndOfGroup: boolean,
    onReleased?: () => void,
    onAbandoned?: AbandonCallback,
  ): OrderedSubgroupRegistration {
    if (!this.#active) {
      throw new ProtocolViolationError('OrderedSubgroupDelivery.registerReserved', 'Delivery generation is retired')
    }
    const state = this.#reservations.get(reservation)
    if (!state) {
      if (this.#abandonedReservations.has(reservation)) {
        return { groupId, subgroupId, containsEndOfGroup, abandoned: true }
      }
      throw new ProtocolViolationError('OrderedSubgroupDelivery.registerReserved', 'Reservation is not active')
    }
    const expectedGroup = this.#admit(groupId)
    if (expectedGroup === undefined) {
      // The reservation was taken while the window was still unanchored and its
      // group turned out to precede the anchor: a pre-window race, not a violation.
      this.#discardReservation(reservation, state, false, 'before-delivery-window')
      return { groupId, subgroupId, containsEndOfGroup, abandoned: true }
    }
    if (this.#isStale(expectedGroup, groupId, subgroupId)) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.registerReserved',
        `Stale subgroup coordinate ${groupId}:${subgroupId}; expected ${expectedGroup}:${this.#expectedSubgroup ?? 'next authored subgroup'}`,
      )
    }
    const key = coordinateKey(groupId, subgroupId)
    if (this.#pending.has(key)) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.registerReserved',
        `Duplicate subgroup coordinate ${groupId}:${subgroupId}`,
      )
    }

    this.#reservations.delete(reservation)
    const registration: OrderedSubgroupRegistration = {
      groupId,
      subgroupId,
      containsEndOfGroup,
      abandoned: false,
    }
    this.#pending.set(key, {
      groupId,
      subgroupId,
      derivesFromFirstObject: true,
      containsEndOfGroup,
      sawEndOfGroup: false,
      completed: false,
      registration,
      objects: [],
      bytes: 0,
      onReleased,
      onAbandoned,
    })
    this.resume()
    return registration
  }

  append(registration: OrderedSubgroupRegistration, object: MoqtObject): boolean {
    if (!this.#active) return false
    if (registration.abandoned) return false

    const key = coordinateKey(registration.groupId, registration.subgroupId)
    const subgroup = this.#pending.get(key)
    if (!subgroup) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.append',
        `Unregistered subgroup coordinate ${registration.groupId}:${registration.subgroupId}`,
      )
    }
    if (subgroup.completed) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.append',
        `Object arrived after subgroup completion ${registration.groupId}:${registration.subgroupId}`,
      )
    }
    if (object.groupId !== registration.groupId || object.subgroupId !== registration.subgroupId) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.append',
        `Object coordinate ${object.groupId}:${object.subgroupId ?? 'none'} does not match subgroup ${registration.groupId}:${registration.subgroupId}`,
      )
    }
    if (subgroup.sawEndOfGroup) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.append',
        `END_OF_GROUP is not the final object in subgroup ${registration.groupId}:${registration.subgroupId}`,
      )
    }

    const bytes = retainedObjectBytes(object)
    if (!this.#makeRoomForObject(registration, bytes)) return false

    subgroup.sawEndOfGroup = object.isEndOfGroup()
    subgroup.objects.push(object)
    subgroup.bytes += bytes
    this.#pendingObjects += 1
    this.#pendingBytes += bytes
    this.#flush()
    return true
  }

  complete(registration: OrderedSubgroupRegistration): boolean {
    if (!this.#active) return false
    if (registration.abandoned) return false

    const key = coordinateKey(registration.groupId, registration.subgroupId)
    const subgroup = this.#pending.get(key)
    if (!subgroup) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.complete',
        `Unregistered subgroup coordinate ${registration.groupId}:${registration.subgroupId}`,
      )
    }
    if (subgroup.completed) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.complete',
        `Subgroup coordinate completed twice ${registration.groupId}:${registration.subgroupId}`,
      )
    }

    subgroup.containsEndOfGroup ||= subgroup.sawEndOfGroup
    subgroup.completed = true
    this.#flush()
    return true
  }

  resume(): void {
    if (!this.#active || !this.#canPromote()) return
    const expectedGroup = this.#expectedGroup
    if (expectedGroup === undefined) return
    if (this.#expectedSubgroup === undefined && !this.#hasReservationForExpectedGroup()) {
      this.#expectedSubgroup = this.#nextPendingSubgroup()
      if (this.#expectedSubgroup === undefined) this.#advanceDeferredEndIfDrained(expectedGroup)
    }
    this.#flush()
  }

  finish(): void {
    if (!this.#active) return
    if (this.#reservations.size > 0) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.finish',
        `Ordered delivery ended with ${this.#reservations.size} subgroup stream(s) awaiting identity`,
      )
    }
    // Unanchored means no stream ever registered a coordinate, so nothing is pending.
    if (this.#expectedGroup === undefined) {
      this.retire()
      return
    }
    if (this.#pending.size > 0 || this.#expectedSubgroup === undefined) {
      throw new ProtocolViolationError(
        'OrderedSubgroupDelivery.finish',
        `Ordered delivery ended before subgroup ${this.#expectedGroup}:${this.#expectedSubgroup ?? 'next authored subgroup'}`,
      )
    }
    this.retire()
  }

  retire(): void {
    if (!this.#active) return
    this.#active = false
    this.#pending.clear()
    this.#reservations.clear()
    this.#pendingObjects = 0
    this.#pendingBytes = 0
  }

  /**
   * Place `groupId` against the delivery window and return the expected group
   * once it is inside, or `undefined` when it precedes the window and must be
   * abandoned. Anchors an unanchored window on `groupId`, and re-anchors a
   * delivery-anchored window down to a lower group while nothing has been
   * released yet.
   */
  #admit(groupId: bigint): bigint | undefined {
    const startGroup = this.#startGroup
    if (startGroup === undefined || (groupId < startGroup && this.#canReanchor())) {
      this.#anchor(groupId)
      return groupId
    }
    // NextGroupStart can race subgroup streams that were already in flight for
    // the publisher's previous group. They are outside this delivery window.
    if (groupId < startGroup) return undefined
    const expectedGroup = this.#expectedGroup
    if (expectedGroup === undefined) throw new Error('Ordered subgroup window anchor is inconsistent')
    return expectedGroup
  }

  #anchor(groupId: bigint): void {
    this.#startGroup = groupId
    this.#expectedGroup = groupId
    this.#expectedSubgroup = 0n
    this.#lastReleasedSubgroup = undefined
  }

  /** The window may still move down while its anchor came from delivery and nothing was released. */
  #canReanchor(): boolean {
    // Eviction may have advanced past and discarded the anchor before any
    // subgroup was released. That commits the ordering just as a release does.
    return this.#anchorsOnArrival && !this.#hasReleased && this.#expectedGroup === this.#startGroup
  }

  #precedesWindow(groupId: bigint): boolean {
    return this.#startGroup !== undefined && groupId < this.#startGroup && !this.#canReanchor()
  }

  #isStale(expectedGroup: bigint, groupId: bigint, subgroupId: bigint): boolean {
    return (
      groupId < expectedGroup ||
      (groupId === expectedGroup &&
        ((this.#expectedSubgroup !== undefined && subgroupId < this.#expectedSubgroup) ||
          (this.#lastReleasedSubgroup !== undefined && subgroupId <= this.#lastReleasedSubgroup)))
    )
  }

  /**
   * Abandon pending subgroups until the window has room again.
   *
   * The window fills because {@link #flush} only releases at
   * `(#expectedGroup, #expectedSubgroup)`. When that coordinate never arrives —
   * e.g. delivery started mid-stream because a joining FETCH was not retained —
   * every later subgroup queues behind it and the window fills with subgroups
   * that can never be released. Observed as 256 distinct groups pending at
   * `2473:0`.
   *
   * This used to throw a ProtocolViolationError, which was wrong twice over: a
   * LOCAL resource bound is not a peer protocol violation, and `client.ts`
   * disconnects on ANY error escaping `handleRecvStreams`, so a full window
   * terminated the session and playback stopped outright
   * (Blockcast/pim-multicast-gateway#2028).
   *
   * Dropping the oldest coordinate frees a slot AND unblocks the head of line,
   * because the expectation advances past the coordinate being waited on. The
   * abandoned subgroup's objects are discarded: a visible gap is the deliberate
   * trade against a terminated session.
   */
  #enforceSubgroupLimit(incoming?: Readonly<{ key: string; groupId: bigint; subgroupId: bigint }>): void {
    while (this.#pending.size + this.#reservations.size > this.#limits.maxPendingSubgroups) {
      const expectedSubgroup = this.#expectedSubgroup
      const incomingIsExpected =
        incoming !== undefined &&
        expectedSubgroup !== undefined &&
        incoming.groupId === this.#expectedGroup &&
        incoming.subgroupId === expectedSubgroup
      const incomingFollowsExpectedReservation =
        incoming !== undefined &&
        expectedSubgroup !== undefined &&
        incoming.groupId === this.#expectedGroup &&
        incoming.subgroupId > expectedSubgroup &&
        this.#hasReservationForExpectedGroup()

      // A newly-arrived awaited head must not be stranded by advancing through
      // later buffered coordinates. Likewise, an unresolved reservation for
      // the awaited head outranks later known coordinates in the same group.
      if (
        (incomingIsExpected || incomingFollowsExpectedReservation) &&
        this.#evictFarthestPending(incomingIsExpected ? incoming.key : undefined)
      ) {
        continue
      }
      const reservationGroup = this.#oldestReservationGroup()
      const pendingGroup = this.#oldestPendingGroup()
      // Prefer an unidentified stream only when its known group is no later
      // than buffered media; skipping a future reservation must not strand an
      // earlier known group behind the expectation.
      if (
        reservationGroup !== undefined &&
        (pendingGroup === undefined || reservationGroup <= pendingGroup) &&
        this.#evictOldestReservation()
      ) {
        continue
      }
      if (this.#evictOldestPending()) continue
      throw new Error('Ordered subgroup capacity accounting is inconsistent')
    }
  }

  #hasReservationForExpectedGroup(): boolean {
    for (const state of this.#reservations.values()) {
      if (state.groupId === this.#expectedGroup) return true
    }
    return false
  }

  /** Discard the farthest known coordinate without moving the authored-order cursor. */
  #evictFarthestPending(protectedKey?: string): boolean {
    let farthestPending: [string, PendingSubgroup] | undefined
    for (const entry of this.#pending) {
      if (entry[0] === protectedKey) continue
      const coordinate = entry[1]
      if (
        farthestPending === undefined ||
        coordinate.groupId > farthestPending[1].groupId ||
        (coordinate.groupId === farthestPending[1].groupId && coordinate.subgroupId > farthestPending[1].subgroupId)
      ) {
        farthestPending = entry
      }
    }
    if (!farthestPending) return false
    this.#discardPending(farthestPending[0], farthestPending[1], false)
    this.#flush()
    return true
  }

  #oldestReservationGroup(): bigint | undefined {
    let oldest: bigint | undefined
    for (const state of this.#reservations.values()) {
      if (oldest === undefined || state.groupId < oldest) oldest = state.groupId
    }
    return oldest
  }

  #oldestPendingGroup(): bigint | undefined {
    let oldest: bigint | undefined
    for (const subgroup of this.#pending.values()) {
      if (oldest === undefined || subgroup.groupId < oldest) oldest = subgroup.groupId
    }
    return oldest
  }

  #makeRoomForObject(registration: OrderedSubgroupRegistration, bytes: number): boolean {
    while (
      this.#pendingObjects >= this.#limits.maxPendingObjects ||
      bytes > this.#limits.maxPendingBytes - this.#pendingBytes
    ) {
      const expectedKey =
        this.#expectedGroup !== undefined && this.#expectedSubgroup !== undefined
          ? coordinateKey(this.#expectedGroup, this.#expectedSubgroup)
          : undefined
      // Successor bytes must not evict the expected stream whose next object
      // would drain immediately. An object larger than the entire limit still
      // abandons its own subgroup when no successor can make enough room.
      if (
        !(expectedKey !== undefined && this.#pending.has(expectedKey) && this.#evictFarthestPending(expectedKey)) &&
        !this.#evictOldestPending()
      )
        return false
      if (registration.abandoned) return false
    }
    return true
  }

  /** Cancel the earliest accepted stream whose subgroup identity is still unknown. */
  #evictOldestReservation(): boolean {
    let oldest: [OrderedSubgroupReservation, OrderedSubgroupReservationState] | undefined
    for (const entry of this.#reservations) {
      if (!oldest || entry[1].groupId < oldest[1].groupId) oldest = entry
    }
    if (!oldest) return false

    this.#discardReservation(oldest[0], oldest[1], true, 'window-pressure')
    this.resume()
    return true
  }

  #discardReservation(
    reservation: OrderedSubgroupReservation,
    state: OrderedSubgroupReservationState,
    advance: boolean,
    reason: OrderedSubgroupAbandonReason,
  ): void {
    this.#reservations.delete(reservation)
    this.#abandonedReservations.add(reservation)
    const expectedGroup = this.#expectedGroup
    if (advance && expectedGroup !== undefined && state.groupId >= expectedGroup) {
      const sameGroupStillPending =
        this.#oldestReservationGroup() === state.groupId ||
        [...this.#pending.values()].some((subgroup) => subgroup.groupId === state.groupId)
      if (state.containsEndOfGroup && !sameGroupStillPending) {
        if (this.#deferredEndOfExpectedGroup === state.groupId) this.#deferredEndOfExpectedGroup = undefined
        this.#expectedGroup = state.groupId + 1n
        this.#expectedSubgroup = 0n
        this.#lastReleasedSubgroup = undefined
      } else {
        if (state.containsEndOfGroup && state.groupId === expectedGroup) {
          this.#deferredEndOfExpectedGroup = state.groupId
        }
        if (state.groupId > expectedGroup) {
          this.#expectedGroup = state.groupId
          this.#lastReleasedSubgroup = undefined
        }
        this.#expectedSubgroup = undefined
      }
    }
    state.onAbandoned?.(reason)
  }

  /** Drop the lowest pending coordinate and resume just past it. */
  #evictOldestPending(): boolean {
    let oldestKey: string | undefined
    let oldest: PendingSubgroup | undefined
    for (const [key, coordinate] of this.#pending) {
      if (
        oldest === undefined ||
        coordinate.groupId < oldest.groupId ||
        (coordinate.groupId === oldest.groupId && coordinate.subgroupId < oldest.subgroupId)
      ) {
        oldestKey = key
        oldest = coordinate
      }
    }
    if (oldestKey === undefined || oldest === undefined) return false

    this.#discardPending(oldestKey, oldest, true)
    this.#flush()
    return true
  }

  #discardPending(key: string, subgroup: PendingSubgroup, advance: boolean): void {
    this.#pending.delete(key)
    this.#pendingObjects -= subgroup.objects.length
    this.#pendingBytes -= subgroup.bytes
    subgroup.registration.abandoned = true
    // A completed transport stream needs no cancellation, and reporting its
    // callback would falsely claim that its discarded objects were delivered.
    if (!subgroup.completed) subgroup.onAbandoned?.('window-pressure')

    if (!advance) return
    // Only advance for a coordinate at or ahead of the expectation. A leftover
    // that is already behind it frees its slot by being dropped; advancing past
    // it would move the expectation backward and re-block delivery.
    const expectedGroup = this.#expectedGroup
    const atOrAfterExpectation =
      expectedGroup !== undefined &&
      (subgroup.groupId > expectedGroup ||
        (subgroup.groupId === expectedGroup &&
          (this.#expectedSubgroup === undefined || subgroup.subgroupId >= this.#expectedSubgroup)))
    if (atOrAfterExpectation) this.#advancePast(subgroup)
  }

  #flush(): void {
    while (this.#active) {
      const expectedGroup = this.#expectedGroup
      const expectedSubgroup = this.#expectedSubgroup
      if (expectedGroup === undefined || expectedSubgroup === undefined) return
      const key = coordinateKey(expectedGroup, expectedSubgroup)
      const subgroup = this.#pending.get(key)
      if (!subgroup) return

      if (subgroup.completed && subgroup.containsEndOfGroup) {
        for (const coordinate of this.#pending.values()) {
          if (coordinate.groupId === expectedGroup && coordinate.subgroupId > expectedSubgroup) {
            throw new ProtocolViolationError(
              'OrderedSubgroupDelivery.flush',
              `Subgroup ${coordinate.groupId}:${coordinate.subgroupId} follows END_OF_GROUP at ${expectedGroup}:${expectedSubgroup}`,
            )
          }
        }
      }

      this.#pendingObjects -= subgroup.objects.length
      this.#pendingBytes -= subgroup.bytes
      const objects = subgroup.objects
      subgroup.objects = []
      subgroup.bytes = 0

      for (const object of objects) {
        this.#hasReleased = true
        this.#enqueue(object)
        if (!this.#active) return
      }
      if (!subgroup.completed) return
      this.#pending.delete(key)
      this.#hasReleased = true
      // FIN is observable only after this subgroup has entered the ordered consumer stream.
      subgroup.onReleased?.()

      this.#advancePast(subgroup)
    }
  }

  /**
   * Advance the expected coordinate past `subgroup`.
   *
   * Shared by {@link #flush} (releasing a subgroup) and
   * {@link #evictOldestPending} (abandoning one) so both use the SAME
   * transition. Eviction previously hardcoded `subgroupId + 1`, which is wrong
   * when the dropped subgroup ends its group: the next valid coordinate is
   * `(groupId + 1, 0)`, and expecting a subgroup that will never exist leaves
   * every later group blocked — the zero-frame symptom the eviction exists to
   * clear.
   *
   * `containsEndOfGroup` is normalised by {@link complete}, so an evicted
   * subgroup also remembers any terminal object already streamed to the consumer.
   */
  #advancePast(subgroup: PendingSubgroup): void {
    if (subgroup.containsEndOfGroup || subgroup.sawEndOfGroup) {
      if (this.#deferredEndOfExpectedGroup === subgroup.groupId) this.#deferredEndOfExpectedGroup = undefined
      this.#expectedGroup = subgroup.groupId + 1n
      this.#expectedSubgroup = 0n
      this.#lastReleasedSubgroup = undefined
      return
    }
    this.#expectedGroup = subgroup.groupId
    if (subgroup.derivesFromFirstObject) {
      this.#lastReleasedSubgroup = subgroup.subgroupId
      const hasExpectedReservation = this.#hasReservationForExpectedGroup()
      const nextPending = !hasExpectedReservation && this.#canPromote() ? this.#nextPendingSubgroup() : undefined
      this.#expectedSubgroup = nextPending
    } else {
      this.#expectedSubgroup = subgroup.subgroupId + 1n
      this.#lastReleasedSubgroup = subgroup.subgroupId
    }
    this.#advanceDeferredEndIfDrained(subgroup.groupId)
  }

  #advanceDeferredEndIfDrained(groupId: bigint): boolean {
    if (
      this.#deferredEndOfExpectedGroup !== groupId ||
      this.#hasReservationForExpectedGroup() ||
      [...this.#pending.values()].some((pending) => pending.groupId === groupId)
    ) {
      return false
    }
    if (!this.#canPromote()) {
      this.#expectedSubgroup = undefined
      return false
    }
    this.#deferredEndOfExpectedGroup = undefined
    this.#expectedGroup = groupId + 1n
    this.#expectedSubgroup = 0n
    this.#lastReleasedSubgroup = undefined
    return true
  }

  #nextPendingSubgroup(): bigint | undefined {
    let nextSubgroup: bigint | undefined
    for (const coordinate of this.#pending.values()) {
      if (
        coordinate.groupId === this.#expectedGroup &&
        (this.#lastReleasedSubgroup === undefined || coordinate.subgroupId > this.#lastReleasedSubgroup) &&
        (nextSubgroup === undefined || coordinate.subgroupId < nextSubgroup)
      ) {
        nextSubgroup = coordinate.subgroupId
      }
    }
    return nextSubgroup
  }
}
