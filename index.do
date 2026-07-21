// Event delivery primitives for Doof programs.

import { Duration } from "std/time"

import class NativeChannel from "native_event.hpp" as doof_event::NativeChannel {
  isolated static createChannel(
    capacity: int,
    highWater: int,
    lowWater: int,
    keepsAlive: bool,
  ): NativeChannel
  isolated registerSenderReady(handler: (): none): none
  isolated registerSenderClosed(handler: (): none): none
  isolated registerReceiverClosed(handler: (): none): none
  isolated tryClose(): bool
}

import isolated function _trySendChannelMessage<T>(
  channel: NativeChannel,
  value: T,
  hasKey: bool,
  key: string,
): int from "native_event.hpp" as doof_event::trySendChannelMessage

import isolated function _registerChannelReceiverMessage<T>(
  channel: NativeChannel,
  handler: (value: T): none,
): none from "native_event.hpp" as doof_event::registerChannelReceiverMessage

import class NativeTimer from "native_event.hpp" as doof_event::NativeTimer {
  isolated static createTimeout(delayNanos: long, keepsAlive: bool, handler: (): none): NativeTimer
  isolated static createInterval(intervalNanos: long, keepsAlive: bool, handler: (): none): NativeTimer
  isolated cancel(): bool
}

import function _runMainEventLoop(): none from "native_event.hpp" as doof_event::runMainEventLoop
import function _drainMainEventLoop(): int from "native_event.hpp" as doof_event::drainMainEventLoop
import function _setMainEventWakeHandler(handler: (): none): none from "native_event.hpp" as doof_event::setMainEventWakeCallback
import function _clearMainEventWakeHandler(): none from "native_event.hpp" as doof_event::clearMainEventWakeHandler

export enum Backpressure {
  None,
  High,
}

export enum SendError {
  Full,
  Closed,
}

export class ChannelSender<T> {
  private readonly native: NativeChannel

  send(value: T, key: string | none = none): Result<Backpressure, SendError> {
    code := if key == none then _trySendChannelMessage(this.native, value, false, "") else _trySendChannelMessage(this.native, value, true, key!)

    return case code {
      0 -> Success { value: Backpressure.None },
      1 -> Success { value: Backpressure.High },
      2 -> Failure { error: SendError.Full },
      _ -> Failure { error: SendError.Closed },
    }
  }

  onReady(handler: (): none): none {
    this.native.registerSenderReady(handler)
  }

  onClosed(handler: (): none): none {
    this.native.registerSenderClosed(handler)
  }

  close(): none {
    this.native.tryClose()
  }
}

export class ChannelReceiver<T> {
  private readonly native: NativeChannel

  onMessage(handler: (it: T): none): none {
    _registerChannelReceiverMessage(this.native, handler)
  }

  onClosed(handler: (): none): none {
    this.native.registerReceiverClosed(handler)
  }

  close(): none {
    this.native.tryClose()
  }
}

export function createChannel<T>(
  capacity: int = 256,
  highWater: int = 0,
  lowWater: int = -1,
  keepsAlive: bool = true,
): Tuple<ChannelSender<T>, ChannelReceiver<T> > {
  if capacity <= 0 {
    panic("Channel capacity must be positive")
  }
  actualHighWater := if highWater == 0 then capacity else highWater
  actualLowWater := if lowWater < 0 then actualHighWater \ 2 else lowWater

  if actualHighWater <= 0 || actualHighWater > capacity {
    panic("Channel highWater must be between 1 and capacity")
  }
  if actualLowWater < 0 || actualLowWater > actualHighWater {
    panic("Channel lowWater must be between 0 and highWater")
  }

  native := NativeChannel.createChannel(capacity, actualHighWater, actualLowWater, keepsAlive)
  return (
    ChannelSender<T> { native },
    ChannelReceiver<T> { native },
  )
}

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

// First-cut explicit pump for hosts that do not yet have runtime integration.
// It blocks efficiently while keep-alive channels remain open, dispatches
// channel handlers on the calling thread, and returns once no keep-alive
// channels remain and the ready queue has drained.
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
