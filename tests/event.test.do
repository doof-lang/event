import { Assert } from "std/assert"
import { Duration } from "std/time"

import {
  AsyncEventChannelError,
  Timer,
  createMainAsyncEventChannel,
  drainMainEventLoop,
  runMainEventLoop,
  setInterval,
  setTimeout,
} from "../index"

export function testMainAsyncEventChannelDispatchesQueuedValues(): void {
  let handled: int[] = []
  events := createMainAsyncEventChannel{
    handler: (event: int): void => handled.push(event),
    capacity: 4,
    keepsAlive: false,
  }

  try! events.send(1)
  try! events.send(2)
  try! events.send(3)

  runMainEventLoop()

  Assert.equal(handled.length, 3)
  Assert.equal(handled[0], 1)
  Assert.equal(handled[1], 2)
  Assert.equal(handled[2], 3)
}

export function testMainAsyncEventChannelReportsFull(): void {
  events := createMainAsyncEventChannel<int>{
    handler: (event: int): void => {},
    capacity: 1,
    keepsAlive: false,
  }

  try! events.send(1)
  overflow := events.send(2)

  case overflow {
    s: Success -> Assert.fail("expected second send to fail")
    f: Failure -> Assert.equal(f.error, AsyncEventChannelError.Full)
  }

  runMainEventLoop()
}

export function testMainAsyncEventChannelReportsClosed(): void {
  events := createMainAsyncEventChannel<int>{
    handler: (event: int): void => {},
    capacity: 1,
    keepsAlive: false,
  }

  try! events.close()
  afterClose := events.send(1)

  case afterClose {
    s: Success -> Assert.fail("expected send after close to fail")
    f: Failure -> Assert.equal(f.error, AsyncEventChannelError.Closed)
  }
}

export function testDrainMainEventLoopDispatchesReadyValuesWithoutBlocking(): void {
  let handled: int[] = []
  events := createMainAsyncEventChannel{
    handler: (event: int): void => handled.push(event),
    capacity: 4,
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
