import { Assert } from "std/assert"
import { Duration } from "std/time"

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
