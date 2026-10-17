import { Assert } from "std/assert"
import { Duration } from "std/time"

import {
  Timer,
  drainMainEventLoop,
  retainMainEventLoop,
  runMainEventLoop,
  setInterval,
  setTimeout,
} from "../index"

class ActorTimerState {
  values: int[] = []
  let timer: Timer | none = none

  startTimeout(): none {
    setTimeout{
      delay: Duration.ZERO,
      handler: (): none => this.values.push(90),
      keepsAlive: true,
    }
  }

  startInterval(): none {
    this.timer = setInterval{
      interval: Duration.ofMillis(1L),
      handler: (): none => {
        this.values.push(91)
        this.timer!.cancel()
      },
      keepsAlive: true,
    }
  }

  count(): int => this.values.length

  at(index: int): int => this.values[index]
}

class ActorDispatcher {
  dispatch(callback: (): none): none {
    callback.dispatch()
  }
}

export function testDrainMainEventLoopDispatchesPostedCallbacksWithoutBlocking(): none {
  let handled: int[] = []
  record := (value: int): none => handled.push(value)

  record.post(10)
  record.post(20)

  Assert.equal(drainMainEventLoop(), 2)
  Assert.equal(handled.length, 2)
  Assert.equal(handled[0], 10)
  Assert.equal(handled[1], 20)
  Assert.equal(drainMainEventLoop(), 0)
}

export function testDrainMainEventLoopReturnsZeroWhenNoWorkIsReady(): none {
  Assert.equal(drainMainEventLoop(), 0)
}

export function testRootCallbackDispatchedFromActorRunsOnMainDrain(): none {
  sender := Actor<ActorDispatcher>()
  let value = 0
  callback := (): none => {
    value = 42
  }

  sender.dispatch(callback)

  Assert.equal(value, 0)
  Assert.equal(drainMainEventLoop(), 1)
  Assert.equal(value, 42)

  retired := retire sender
}

export function testMainEventLoopHoldKeepsLoopRunningUntilReleased(): none {
  hold := retainMainEventLoop()
  let fired = false
  timer := setTimeout{
    delay: Duration.ofMillis(5L),
    handler: (): none => {
      fired = true
      hold.release()
    },
    keepsAlive: false,
  }

  runMainEventLoop()

  Assert.isTrue(fired)
  Assert.isFalse(timer.cancel())
}

export function testMainEventLoopHoldReleasesOnlyOnce(): none {
  hold := retainMainEventLoop()

  Assert.isTrue(hold.release())
  Assert.isFalse(hold.release())
  runMainEventLoop()
}

export function testTimeoutCreatedInsideActorDispatchesOnOwningActor(): none {
  owner := Actor<ActorTimerState>()

  owner.startTimeout()
  runMainEventLoop()

  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 90)

  retired := retire owner
}

export function testIntervalCreatedInsideActorDispatchesOnOwningActor(): none {
  owner := Actor<ActorTimerState>()

  owner.startInterval()
  runMainEventLoop()

  Assert.equal(owner.count(), 1)
  Assert.equal(owner.at(0), 91)

  retired := retire owner
}

export function testTimeoutFiresOnce(): none {
  let fired = 0
  timer := setTimeout{
    delay: Duration.ofMillis(1L),
    handler: (): none => {
      fired = fired + 1
    },
  }

  runMainEventLoop()

  Assert.equal(fired, 1)
  Assert.isFalse(timer.cancel())
}

export function testZeroDelayTimeoutRunsOnNextDrain(): none {
  let fired = false
  timer := setTimeout{
    delay: Duration.ZERO,
    handler: (): none => {
      fired = true
    },
  }

  Assert.isFalse(fired)
  runMainEventLoop()
  Assert.isTrue(fired)
  Assert.isFalse(timer.cancel())
}

export function testCancelBeforeTimeoutFiresPreventsCallback(): none {
  let fired = false
  timer := setTimeout{
    delay: Duration.ofMillis(50L),
    handler: (): none => {
      fired = true
    },
  }

  Assert.isTrue(timer.cancel())
  runMainEventLoop()

  Assert.isFalse(fired)
}

export function testRepeatedCancelReportsOnlyFirstCancellation(): none {
  timer := setTimeout{
    delay: Duration.ofMillis(50L),
    handler: (): none => {},
  }

  Assert.isTrue(timer.cancel())
  Assert.isFalse(timer.cancel())
  runMainEventLoop()
}

export function testCancelAfterTimeoutFiredReturnsFalse(): none {
  let fired = false
  timer := setTimeout{
    delay: Duration.ZERO,
    handler: (): none => {
      fired = true
    },
  }

  runMainEventLoop()

  Assert.isTrue(fired)
  Assert.isFalse(timer.cancel())
}

export function testIntervalFiresRepeatedlyAndCancelsItself(): none {
  let fired = 0
  let timers: Timer[] = []

  timer := setInterval{
    interval: Duration.ofMillis(1L),
    handler: (): none => {
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
