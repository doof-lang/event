import { Assert } from "std/assert"
import { Duration, Thread } from "std/time"

import {
  Backpressure,
  Channel,
  ChannelClosed,
  ChannelMessage,
  ChannelReady,
  SendError,
  Timer,
  createChannel,
  drainMainEventLoop,
  runMainEventLoop,
  setInterval,
  setTimeout,
} from "../index"

function collectIntChannelEvents(target: int[]): (event: ChannelMessage<int> | ChannelReady<int> | ChannelClosed<int>): void {
  return (event: ChannelMessage<int> | ChannelReady<int> | ChannelClosed<int>): void => {
    case event {
      message: ChannelMessage<int> -> target.push(message.value)
      _: ChannelReady<int> -> target.push(-1)
      _: ChannelClosed<int> -> target.push(-2)
    }
  }
}

class ActorChannelState {
  values: int[] = []
  timer: Timer | null = null

  openChannel(
    capacity: int = 4,
    highWater: int = 4,
    lowWater: int = 2,
  ): Channel<int> {
    return createChannel<int>{
      handler: (event: ChannelMessage<int> | ChannelReady<int> | ChannelClosed<int>): void => {
        case event {
          message: ChannelMessage<int> -> this.values.push(message.value)
          _: ChannelReady<int> -> this.values.push(-1)
          _: ChannelClosed<int> -> this.values.push(-2)
        }
      },
      capacity,
      highWater,
      lowWater,
      keepsAlive: false,
    }
  }

  mark(value: int): void {
    this.values.push(value)
  }

  sleepMillis(millis: long): void {
    Thread.sleep(Duration.ofMillis(millis))
  }

  startTimeout(): void {
    setTimeout{
      delay: Duration.ZERO,
      handler: (): void => this.values.push(90),
      keepsAlive: true,
    }
  }

  startInterval(): void {
    this.timer = setInterval{
      interval: Duration.ofMillis(1L),
      handler: (): void => {
        this.values.push(91)
        this.timer!.cancel()
      },
      keepsAlive: true,
    }
  }

  count(): int => this.values.length

  at(index: int): int => this.values[index]
}

class ActorChannelSender {
  deliver(events: Channel<int>, value: int): void {
    try! events.send(value)
  }

  dispatch(callback: (): void): void {
    callback.dispatch()
  }
}

export function testDrainMainEventLoopDispatchesReadyValuesWithoutBlocking(): void {
  let handled: int[] = []
  events := createChannel<int>{
    handler: collectIntChannelEvents(handled),
    capacity: 4,
    highWater: 4,
    lowWater: 2,
    keepsAlive: false,
  }

  try! events.send(10)
  try! events.send(20)

  dispatched := drainMainEventLoop()

  Assert.equal(dispatched, 2)
  Assert.equal(handled.length, 2)
  Assert.equal(handled[0], 10)
  Assert.equal(handled[1], 20)
  Assert.equal(drainMainEventLoop(), 0)
}

export function testDrainMainEventLoopReturnsZeroWhenNoWorkIsReady(): void {
  Assert.equal(drainMainEventLoop(), 0)
}

export function testActorOwnedChannelDispatchesMessageOnOwningActor(): void {
  owner := Actor<ActorChannelState>()
  events := owner.openChannel()

  try! events.send(42)
  Assert.equal(drainMainEventLoop(), 0)

  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 42)

  events.close()
  Assert.equal(owner.count(), 2)
  retired := retire owner
}

export function testActorOwnedChannelPreservesMailboxOrdering(): void {
  owner := Actor<ActorChannelState>()
  events := owner.openChannel()

  first := async owner.mark(1)
  try! events.send(2)
  Assert.equal(drainMainEventLoop(), 0)
  last := async owner.mark(3)

  try! first.get()
  try! last.get()

  Assert.equal(owner.count(), 3)
  Assert.equal(owner.at(0), 1)
  Assert.equal(owner.at(1), 2)
  Assert.equal(owner.at(2), 3)

  events.close()
  Assert.equal(owner.count(), 4)
  retired := retire owner
}

export function testActorOwnedChannelDispatchesReadyAndClosedOnOwningActor(): void {
  owner := Actor<ActorChannelState>()
  events := owner.openChannel(3, 2, 0)

  try! events.send(10)
  high := try! events.send(20)
  Assert.equal(high, Backpressure.High)
  events.close()

  Assert.equal(owner.count(), 4)
  Assert.equal(owner.at(0), 10)
  Assert.equal(owner.at(1), 20)
  Assert.equal(owner.at(2), -1)
  Assert.equal(owner.at(3), -2)

  retired := retire owner
}

export function testActorOwnedChannelAcceptsSendFromAnotherActor(): void {
  owner := Actor<ActorChannelState>()
  sender := Actor<ActorChannelSender>()
  events := owner.openChannel()

  sender.deliver(events, 77)
  Assert.equal(drainMainEventLoop(), 0)

  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 77)

  events.close()
  Assert.equal(owner.count(), 2)
  retiredSender := retire sender
  retiredOwner := retire owner
}

export function testRootCallbackDispatchedFromActorRunsOnMainDrain(): void {
  sender := Actor<ActorChannelSender>()
  let value = 0
  callback := (): void => {
    value = 42
  }

  sender.dispatch(callback)

  Assert.equal(value, 0)
  Assert.equal(drainMainEventLoop(), 1)
  Assert.equal(value, 42)

  retired := retire sender
}

export function testActorOwnedChannelDoesNotNeedMainDrainWhileMainSleeps(): void {
  owner := Actor<ActorChannelState>()
  events := owner.openChannel()

  try! events.send(42)
  Thread.sleep(Duration.ofMillis(20L))

  Assert.equal(drainMainEventLoop(), 0)
  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 42)

  events.close()
  Assert.equal(owner.count(), 2)
  retired := retire owner
}

export function testActorOwnedChannelBackpressureClearsOnlyAfterActorPumpRuns(): void {
  owner := Actor<ActorChannelState>()
  events := owner.openChannel(2, 2, 1)
  blocker := async owner.sleepMillis(40L)

  first := try! events.send(1)
  second := try! events.send(2)
  overflow := events.send(3)

  Assert.equal(first, Backpressure.None)
  Assert.equal(second, Backpressure.High)
  case overflow {
    s: Success -> Assert.fail("expected third send to fail")
    f: Failure -> Assert.equal(f.error, SendError.Full)
  }

  try! blocker.get()
  Assert.equal(owner.count(), 3)
  Assert.equal(owner.at(0), 1)
  Assert.equal(owner.at(1), -1)
  Assert.equal(owner.at(2), 2)

  events.close()
  Assert.equal(owner.count(), 4)
  retired := retire owner
}

export function testActorOwnedChannelPumpYieldsAfterBoundedBatch(): void {
  owner := Actor<ActorChannelState>()
  events := owner.openChannel(64, 64, 32)
  blocker := async owner.sleepMillis(40L)

  let index = 0
  while index < 40 {
    try! events.send(index)
    index = index + 1
  }

  marker := async owner.mark(999)
  try! blocker.get()
  try! marker.get()

  Assert.equal(owner.count(), 41)
  Assert.equal(owner.at(31), 31)
  Assert.equal(owner.at(32), 999)
  Assert.equal(owner.at(33), 32)
  Assert.equal(owner.at(40), 39)

  events.close()
  Assert.equal(owner.count(), 42)
  retired := retire owner
}

export function testTimeoutCreatedInsideActorDispatchesOnOwningActor(): void {
  owner := Actor<ActorChannelState>()

  owner.startTimeout()
  runMainEventLoop()

  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 90)

  retired := retire owner
}

export function testIntervalCreatedInsideActorDispatchesOnOwningActor(): void {
  owner := Actor<ActorChannelState>()

  owner.startInterval()
  runMainEventLoop()

  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 91)

  retired := retire owner
}

export function testChannelDispatchesQueuedMessages(): void {
  let handled: int[] = []
  let events: Channel<int> = createChannel<int>{
    handler: collectIntChannelEvents(handled),
    capacity: 4,
    highWater: 3,
    lowWater: 1,
    keepsAlive: false,
  }

  try! events.send(1)
  try! events.send(2)
  try! events.send(3)

  runMainEventLoop()

  Assert.equal(handled.length, 4)
  Assert.equal(handled[0], 1)
  Assert.equal(handled[1], 2)
  Assert.equal(handled[2], -1)
  Assert.equal(handled[3], 3)
}

export function testChannelReportsBackpressureAndFull(): void {
  let events: Channel<int> = createChannel<int>{
    handler: (event: ChannelMessage<int> | ChannelReady<int> | ChannelClosed<int>): void => {},
    capacity: 2,
    highWater: 2,
    lowWater: 1,
    keepsAlive: false,
  }

  first := try! events.send(1)
  second := try! events.send(2)
  overflow := events.send(3)

  Assert.equal(first, Backpressure.None)
  Assert.equal(second, Backpressure.High)
  case overflow {
    s: Success -> Assert.fail("expected third send to fail")
    f: Failure -> Assert.equal(f.error, SendError.Full)
  }

  runMainEventLoop()
}

export function testChannelCoalescesPendingMessagesByKey(): void {
  let handled: int[] = []
  let events: Channel<int> = createChannel<int>{
    handler: collectIntChannelEvents(handled),
    capacity: 2,
    highWater: 2,
    lowWater: 1,
    keepsAlive: false,
  }

  try! events.send(1, "same")
  try! events.send(2, "other")
  overflow := events.send(3)
  coalesced := try! events.send(4, "same")

  case overflow {
    s: Success -> Assert.fail("expected unkeyed send to fail")
    f: Failure -> Assert.equal(f.error, SendError.Full)
  }
  Assert.equal(coalesced, Backpressure.High)

  runMainEventLoop()

  Assert.equal(handled.length, 3)
  Assert.equal(handled[0], 4)
  Assert.equal(handled[1], -1)
  Assert.equal(handled[2], 2)
}

export function testChannelReadyFiresOncePerHighWaterRecovery(): void {
  let handled: int[] = []
  let events: Channel<int> = createChannel<int>{
    handler: collectIntChannelEvents(handled),
    capacity: 4,
    highWater: 3,
    lowWater: 1,
    keepsAlive: false,
  }

  try! events.send(1)
  try! events.send(2)
  high := try! events.send(3)

  Assert.equal(high, Backpressure.High)
  runMainEventLoop()

  Assert.equal(handled.length, 4)
  Assert.equal(handled[0], 1)
  Assert.equal(handled[1], 2)
  Assert.equal(handled[2], -1)
  Assert.equal(handled[3], 3)

  try! events.send(4)
  runMainEventLoop()

  Assert.equal(handled.length, 5)
  Assert.equal(handled[4], 4)
}

export function testChannelCloseDrainsThenDeliversClosed(): void {
  let handled: int[] = []
  let events: Channel<int> = createChannel<int>{
    handler: collectIntChannelEvents(handled),
    capacity: 4,
    highWater: 3,
    lowWater: 1,
    keepsAlive: false,
  }

  try! events.send(1)
  try! events.send(2)
  events.close()
  events.close()
  afterClose := events.send(3)

  case afterClose {
    s: Success -> Assert.fail("expected send after close to fail")
    f: Failure -> Assert.equal(f.error, SendError.Closed)
  }

  runMainEventLoop()

  Assert.equal(handled.length, 3)
  Assert.equal(handled[0], 1)
  Assert.equal(handled[1], 2)
  Assert.equal(handled[2], -2)
}

export function testTimeoutFiresOnce(): void {
  let fired = 0
  timer := setTimeout{
    delay: Duration.ofMillis(1L),
    handler: (): void => {
      fired = fired + 1
    },
  }

  runMainEventLoop()

  Assert.equal(fired, 1)
  Assert.isFalse(timer.cancel())
}

export function testZeroDelayTimeoutRunsOnNextDrain(): void {
  let fired = false
  timer := setTimeout{
    delay: Duration.ZERO,
    handler: (): void => {
      fired = true
    },
  }

  Assert.isFalse(fired)
  runMainEventLoop()
  Assert.isTrue(fired)
  Assert.isFalse(timer.cancel())
}

export function testCancelBeforeTimeoutFiresPreventsCallback(): void {
  let fired = false
  timer := setTimeout{
    delay: Duration.ofMillis(50L),
    handler: (): void => {
      fired = true
    },
  }

  Assert.isTrue(timer.cancel())
  runMainEventLoop()

  Assert.isFalse(fired)
}

export function testRepeatedCancelReportsOnlyFirstCancellation(): void {
  timer := setTimeout{
    delay: Duration.ofMillis(50L),
    handler: (): void => {},
  }

  Assert.isTrue(timer.cancel())
  Assert.isFalse(timer.cancel())
  runMainEventLoop()
}

export function testCancelAfterTimeoutFiredReturnsFalse(): void {
  let fired = false
  timer := setTimeout{
    delay: Duration.ZERO,
    handler: (): void => {
      fired = true
    },
  }

  runMainEventLoop()

  Assert.isTrue(fired)
  Assert.isFalse(timer.cancel())
}

export function testIntervalFiresRepeatedlyAndCancelsItself(): void {
  let fired = 0
  let timers: Timer[] = []

  timer := setInterval{
    interval: Duration.ofMillis(1L),
    handler: (): void => {
      fired = fired + 1
      if fired == 3 {
        Assert.isTrue(timers[0].cancel())
      }
    },
  }
  timers.push(timer)

  runMainEventLoop()

  Assert.equal(fired, 3)
  Assert.isFalse(timer.cancel())
}
