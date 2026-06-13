import { Assert } from "std/assert"
import { Duration, Thread } from "std/time"

import {
  Backpressure,
  ChannelReceiver,
  ChannelSender,
  SendError,
  Timer,
  createChannel,
  drainMainEventLoop,
  runMainEventLoop,
  setInterval,
  setTimeout,
} from "../index"

function collectIntMessages(target: int[]): (value: int): void {
  return (value: int): void => {
    target.push(value)
  }
}

class ActorChannelState {
  values: int[] = []
  timer: Timer | null = null

  openChannel(
    capacity: int = 4,
    highWater: int = 4,
    lowWater: int = 2,
  ): Tuple<ChannelSender<int>, ChannelReceiver<int> > {
    (sender, receiver) := createChannel<int>{
      capacity,
      highWater,
      lowWater,
      keepsAlive: false,
    }
    receiver.onMessage() {
       this.values.push(it)
    }
    receiver.onClosed() {
      this.values.push(-2)
    }
    return (sender, receiver)
  }

  attachReceiver(receiver: ChannelReceiver<int>): void {
    receiver.onMessage((value: int): void => this.values.push(value))
    receiver.onClosed((): void => this.values.push(-2))
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
  values: int[] = []

  attachSender(sender: ChannelSender<int>): void {
    sender.onReady() { this.values.push(-1) }
    sender.onClosed() { this.values.push(-2) }
  }

  deliver(sender: ChannelSender<int>, value: int): void {
    try! sender.send(value)
  }

  dispatch(callback: (): void): void {
    callback.dispatch()
  }

  count(): int => this.values.length

  at(index: int): int => this.values[index]
}

export function testDrainMainEventLoopDispatchesReadyValuesWithoutBlocking(): void {
  let handled: int[] = []
  (sender, receiver) := createChannel<int>{
    capacity: 4,
    highWater: 4,
    lowWater: 2,
    keepsAlive: false,
  }
  receiver.onMessage(collectIntMessages(handled))

  try! sender.send(10)
  try! sender.send(20)

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
  (sender, receiver) := owner.openChannel()

  try! sender.send(42)
  Assert.equal(drainMainEventLoop(), 0)

  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 42)

  receiver.close()
  Assert.equal(owner.count(), 2)
  retired := retire owner
}

export function testActorOwnedChannelPreservesMailboxOrdering(): void {
  owner := Actor<ActorChannelState>()
  (sender, receiver) := owner.openChannel()

  first := async owner.mark(1)
  try! sender.send(2)
  Assert.equal(drainMainEventLoop(), 0)
  last := async owner.mark(3)

  try! first.get()
  try! last.get()

  Assert.equal(owner.count(), 3)
  Assert.equal(owner.at(0), 1)
  Assert.equal(owner.at(1), 2)
  Assert.equal(owner.at(2), 3)

  receiver.close()
  Assert.equal(owner.count(), 4)
  retired := retire owner
}

export function testActorOwnedChannelDispatchesClosedOnOwningReceiver(): void {
  owner := Actor<ActorChannelState>()
  (sender, receiver) := owner.openChannel(3, 2, 0)

  try! sender.send(10)
  high := try! sender.send(20)
  Assert.equal(high, Backpressure.High)
  receiver.close()

  Assert.equal(owner.count(), 3)
  Assert.equal(owner.at(0), 10)
  Assert.equal(owner.at(1), 20)
  Assert.equal(owner.at(2), -2)

  retired := retire owner
}

export function testActorOwnedChannelAcceptsSendFromAnotherActor(): void {
  owner := Actor<ActorChannelState>()
  senderActor := Actor<ActorChannelSender>()
  (sender, receiver) := owner.openChannel()

  senderActor.deliver(sender, 77)
  Assert.equal(drainMainEventLoop(), 0)

  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 77)

  receiver.close()
  Assert.equal(owner.count(), 2)
  retiredSender := retire senderActor
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
  (sender, receiver) := owner.openChannel()

  try! sender.send(42)
  Thread.sleep(Duration.ofMillis(20L))

  Assert.equal(drainMainEventLoop(), 0)
  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 42)

  receiver.close()
  Assert.equal(owner.count(), 2)
  retired := retire owner
}

export function testActorOwnedChannelBackpressureClearsOnlyAfterActorPumpRuns(): void {
  owner := Actor<ActorChannelState>()
  senderActor := Actor<ActorChannelSender>()
  (sender, receiver) := owner.openChannel(2, 2, 1)
  senderActor.attachSender(sender)
  blocker := async owner.sleepMillis(40L)

  first := try! sender.send(1)
  second := try! sender.send(2)
  overflow := sender.send(3)

  Assert.equal(first, Backpressure.None)
  Assert.equal(second, Backpressure.High)
  case overflow {
    s: Success -> Assert.fail("expected third send to fail")
    f: Failure -> Assert.equal(f.error, SendError.Full)
  }

  try! blocker.get()
  Assert.equal(owner.count(), 2)
  Assert.equal(owner.at(0), 1)
  Assert.equal(owner.at(1), 2)
  Assert.equal(senderActor.count(), 1)
  Assert.equal(senderActor.at(0), -1)

  receiver.close()
  Assert.equal(owner.count(), 3)
  Assert.equal(senderActor.count(), 2)
  retired := retire owner
  retiredSender := retire senderActor
}

export function testActorOwnedChannelPumpYieldsAfterBoundedBatch(): void {
  owner := Actor<ActorChannelState>()
  (sender, receiver) := owner.openChannel(64, 64, 32)
  blocker := async owner.sleepMillis(40L)

  let index = 0
  while index < 40 {
    try! sender.send(index)
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

  receiver.close()
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
  (sender, receiver) := createChannel<int>{
    capacity: 4,
    highWater: 3,
    lowWater: 1,
    keepsAlive: false,
  }
  receiver.onMessage(collectIntMessages(handled))

  try! sender.send(1)
  try! sender.send(2)
  try! sender.send(3)

  runMainEventLoop()

  Assert.equal(handled.length, 3)
  Assert.equal(handled[0], 1)
  Assert.equal(handled[1], 2)
  Assert.equal(handled[2], 3)
}

export function testChannelBuffersMessagesUntilReceiverRegisters(): void {
  let handled: int[] = []
  (sender, receiver) := createChannel<int>{
    capacity: 4,
    highWater: 4,
    lowWater: 2,
    keepsAlive: false,
  }

  try! sender.send(10)
  try! sender.send(20)

  Assert.equal(drainMainEventLoop(), 0)
  Assert.equal(handled.length, 0)

  receiver.onMessage(collectIntMessages(handled))
  runMainEventLoop()

  Assert.equal(handled.length, 2)
  Assert.equal(handled[0], 10)
  Assert.equal(handled[1], 20)
}

export function testChannelReportsBackpressureAndFull(): void {
  (sender, receiver) := createChannel<int>{
    capacity: 2,
    highWater: 2,
    lowWater: 1,
    keepsAlive: false,
  }
  receiver.onMessage((value: int): void => {})

  first := try! sender.send(1)
  second := try! sender.send(2)
  overflow := sender.send(3)

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
  let ready: int[] = []
  (sender, receiver) := createChannel<int>{
    capacity: 2,
    highWater: 2,
    lowWater: 1,
    keepsAlive: false,
  }
  receiver.onMessage(collectIntMessages(handled))
  sender.onReady((): void => ready.push(-1))

  try! sender.send(1, "same")
  try! sender.send(2, "other")
  overflow := sender.send(3)
  coalesced := try! sender.send(4, "same")

  case overflow {
    s: Success -> Assert.fail("expected unkeyed send to fail")
    f: Failure -> Assert.equal(f.error, SendError.Full)
  }
  Assert.equal(coalesced, Backpressure.High)

  runMainEventLoop()

  Assert.equal(handled.length, 2)
  Assert.equal(handled[0], 4)
  Assert.equal(handled[1], 2)
  Assert.equal(ready.length, 1)
  Assert.equal(ready[0], -1)
}

export function testChannelReadyFiresOncePerHighWaterRecovery(): void {
  let handled: int[] = []
  let ready: int[] = []
  (sender, receiver) := createChannel<int>{
    capacity: 4,
    highWater: 3,
    lowWater: 1,
    keepsAlive: false,
  }
  receiver.onMessage(collectIntMessages(handled))
  sender.onReady() { ready.push(-1) }

  try! sender.send(1)
  try! sender.send(2)
  high := try! sender.send(3)

  Assert.equal(high, Backpressure.High)
  runMainEventLoop()

  Assert.equal(handled.length, 3)
  Assert.equal(handled[0], 1)
  Assert.equal(handled[1], 2)
  Assert.equal(handled[2], 3)
  Assert.equal(ready.length, 1)
  Assert.equal(ready[0], -1)

  try! sender.send(4)
  runMainEventLoop()

  Assert.equal(handled.length, 4)
  Assert.equal(handled[3], 4)
  Assert.equal(ready.length, 1)
}

export function testChannelReadyWaitsUntilSenderRegistersHandler(): void {
  let handled: int[] = []
  let ready: int[] = []
  (sender, receiver) := createChannel<int>{
    capacity: 4,
    highWater: 3,
    lowWater: 1,
    keepsAlive: false,
  }
  receiver.onMessage(collectIntMessages(handled))

  try! sender.send(1)
  try! sender.send(2)
  high := try! sender.send(3)
  Assert.equal(high, Backpressure.High)

  runMainEventLoop()
  Assert.equal(handled.length, 3)
  Assert.equal(ready.length, 0)

  sender.onReady((): void => ready.push(-1))
  Assert.equal(drainMainEventLoop(), 1)
  Assert.equal(ready.length, 1)
  Assert.equal(ready[0], -1)
}

export function testChannelSenderClosedDoesNotWaitForUnregisteredReady(): void {
  let handled: int[] = []
  let senderClosed: int[] = []
  (sender, receiver) := createChannel<int>{
    capacity: 4,
    highWater: 3,
    lowWater: 1,
    keepsAlive: false,
  }
  receiver.onMessage(collectIntMessages(handled))

  try! sender.send(1)
  try! sender.send(2)
  high := try! sender.send(3)
  Assert.equal(high, Backpressure.High)
  runMainEventLoop()

  sender.close()
  sender.onClosed((): void => senderClosed.push(-3))
  drainMainEventLoop()
  Assert.equal(senderClosed.length, 1)
  Assert.equal(senderClosed[0], -3)
}

export function testChannelCloseDrainsThenDeliversClosed(): void {
  let handled: int[] = []
  let senderClosed: int[] = []
  (sender, receiver) := createChannel<int>{
    capacity: 4,
    highWater: 3,
    lowWater: 1,
    keepsAlive: false,
  }
  receiver.onMessage(collectIntMessages(handled))
  receiver.onClosed((): void => handled.push(-2))
  sender.onClosed((): void => senderClosed.push(-3))

  try! sender.send(1)
  try! sender.send(2)
  receiver.close()
  sender.close()
  afterClose := sender.send(3)

  case afterClose {
    s: Success -> Assert.fail("expected send after close to fail")
    f: Failure -> Assert.equal(f.error, SendError.Closed)
  }

  runMainEventLoop()

  Assert.equal(handled.length, 3)
  Assert.equal(handled[0], 1)
  Assert.equal(handled[1], 2)
  Assert.equal(handled[2], -2)
  Assert.equal(senderClosed.length, 1)
  Assert.equal(senderClosed[0], -3)
}

export function testChannelClosedWaitsUntilEndpointHandlersRegister(): void {
  let receiverClosed: int[] = []
  let senderClosed: int[] = []
  (sender, receiver) := createChannel<int>{
    capacity: 4,
    highWater: 4,
    lowWater: 2,
    keepsAlive: false,
  }

  sender.close()
  Assert.equal(drainMainEventLoop(), 0)

  receiver.onClosed((): void => receiverClosed.push(-2))
  sender.onClosed((): void => senderClosed.push(-3))
  runMainEventLoop()

  Assert.equal(receiverClosed.length, 1)
  Assert.equal(receiverClosed[0], -2)
  Assert.equal(senderClosed.length, 1)
  Assert.equal(senderClosed[0], -3)
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
