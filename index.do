// Event delivery primitives for Doof programs.

import { Duration } from "std/time"

import isolated class NativeTimer from "native_event.hpp" as doof_event::NativeTimer {
  isolated static createTimeout(delayNanos: long, keepsAlive: bool, handler: (): none): NativeTimer
  isolated static createInterval(intervalNanos: long, keepsAlive: bool, handler: (): none): NativeTimer
  isolated cancel(): bool
}

import isolated class NativeMainEventLoopHold from "native_event.hpp" as doof_event::NativeMainEventLoopHold {
  isolated static retain(): NativeMainEventLoopHold
  isolated release(): bool
}

import function _runMainEventLoop(): none from "native_event.hpp" as doof_event::runMainEventLoop
import function _drainMainEventLoop(): int from "native_event.hpp" as doof_event::drainMainEventLoop
import function _setMainEventWakeHandler(handler: (): none): none from "native_event.hpp" as doof_event::setMainEventWakeCallback
import function _clearMainEventWakeHandler(): none from "native_event.hpp" as doof_event::clearMainEventWakeHandler

export class Timer {
  private readonly native: NativeTimer

  cancel(): bool {
    return this.native.cancel()
  }
}

export function setTimeout(
  delay: Duration,
  handler: (): none,
  keepsAlive: bool = true,
): Timer {
  if delay.isNegative() {
    panic("setTimeout delay must not be negative")
  }

  timerHandler := handler
  return Timer(NativeTimer.createTimeout(delay.toNanos(), keepsAlive, (): none => timerHandler.call()))
}

export function setInterval(
  interval: Duration,
  handler: (): none,
  keepsAlive: bool = true,
): Timer {
  if interval.toNanos() <= 0L {
    panic("setInterval interval must be positive")
  }

  timerHandler := handler
  return Timer(NativeTimer.createInterval(interval.toNanos(), keepsAlive, (): none => timerHandler.call()))
}

export class MainEventLoopHold {
  private readonly native: NativeMainEventLoopHold

  // Returns `true` only for the call that released the hold.
  release(): bool {
    return this.native.release()
  }
}

// Keeps `runMainEventLoop()` running until the hold is released or dropped.
export function retainMainEventLoop(): MainEventLoopHold {
  return MainEventLoopHold(NativeMainEventLoopHold.retain())
}

// Explicit pump for hosts that do not yet have runtime integration. It
// blocks efficiently while keep-alive sources (timers, holds, open servers
// and connections) remain, dispatches root-actor work on the calling thread,
// and returns once none remain and the mailbox has drained.
export function runMainEventLoop(): none {
  _runMainEventLoop()
}

// Drains all currently-ready main-loop work without blocking.
// This is intended for OS-owned hosts that need to integrate std/event work
// into another event loop.
export function drainMainEventLoop(): int {
  return _drainMainEventLoop()
}

export function setMainEventWakeHandler(handler: (): none): none {
  _setMainEventWakeHandler(handler)
}

export function clearMainEventWakeHandler(): none {
  _clearMainEventWakeHandler()
}
