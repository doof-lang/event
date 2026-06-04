// Event delivery primitives for Doof programs.

import { Duration } from "std/time"

import class NativeChannel from "native_event.hpp" as doof_event::NativeChannel {
  static createChannel(
    capacity: int,
    highWater: int,
    lowWater: int,
    keepsAlive: bool,
    readyHandler: (): void,
    closedHandler: (): void,
  ): NativeChannel
  trySendMessage(task: (): void, hasKey: bool, key: string): int
  tryClose(): bool
}

import class NativeTimer from "native_event.hpp" as doof_event::NativeTimer {
  static createTimeout(delayNanos: long, keepsAlive: bool, handler: (): void): NativeTimer
  static createInterval(intervalNanos: long, keepsAlive: bool, handler: (): void): NativeTimer
  cancel(): bool
}

import function _runMainEventLoop(): void from "native_event.hpp" as doof_event::runMainEventLoop
import function _drainMainEventLoop(): int from "native_event.hpp" as doof_event::drainMainEventLoop
import function _setMainEventWakeHandler(handler: (): void): void from "native_event.hpp" as doof_event::setMainEventWakeCallback
import function _clearMainEventWakeHandler(): void from "native_event.hpp" as doof_event::clearMainEventWakeHandler

export enum Backpressure {
  None,
  High,
}

export enum SendError {
  Full,
  Closed,
}

export class ChannelMessage<T> {
  readonly value: T
}

export class ChannelReady<T> {}

export class ChannelClosed<T> {}

export class Channel<T> {
  private readonly native: NativeChannel
  private readonly handler: (event: ChannelMessage<T> | ChannelReady<T> | ChannelClosed<T>): void

  send(value: T, key: string | null = null): Result<Backpressure, SendError> {
    eventHandler := this.handler
    code := if key == null then this.native.trySendMessage(
      (): void => eventHandler.dispatch(ChannelMessage<T> { value }),
      false,
      "",
    ) else this.native.trySendMessage(
      (): void => eventHandler.dispatch(ChannelMessage<T> { value }),
      true,
      key!,
    )

    return case code {
      0 -> Success { value: Backpressure.None },
      1 -> Success { value: Backpressure.High },
      2 -> Failure { error: SendError.Full },
      _ -> Failure { error: SendError.Closed },
    }
  }

  close(): void {
    this.native.tryClose()
  }
}

export function createChannel<T>(
  handler: (event: ChannelMessage<T> | ChannelReady<T> | ChannelClosed<T>): void,
  capacity: int = 256,
  highWater: int = 0,
  lowWater: int = -1,
  keepsAlive: bool = true,
): Channel<T> {
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

  eventHandler := handler
  return Channel<T> {
    native: NativeChannel.createChannel(
      capacity,
      actualHighWater,
      actualLowWater,
      keepsAlive,
      (): void => eventHandler.dispatch(ChannelReady<T> {}),
      (): void => eventHandler.dispatch(ChannelClosed<T> {}),
    ),
    handler,
  }
}

export class Timer {
  private readonly native: NativeTimer

  cancel(): bool {
    return this.native.cancel()
  }
}

export function setTimeout(
  delay: Duration,
  handler: (): void,
  keepsAlive: bool = true,
): Timer {
  if delay.isNegative() {
    panic("setTimeout delay must not be negative")
  }

  timerHandler := handler
  return Timer(NativeTimer.createTimeout(delay.toNanos(), keepsAlive, (): void => timerHandler.dispatch()))
}

export function setInterval(
  interval: Duration,
  handler: (): void,
  keepsAlive: bool = true,
): Timer {
  if interval.toNanos() <= 0L {
    panic("setInterval interval must be positive")
  }

  timerHandler := handler
  return Timer(NativeTimer.createInterval(interval.toNanos(), keepsAlive, (): void => timerHandler.dispatch()))
}

// First-cut explicit pump for hosts that do not yet have runtime integration.
// It blocks efficiently while keep-alive channels remain open, dispatches
// channel handlers on the calling thread, and returns once no keep-alive
// channels remain and the ready queue has drained.
export function runMainEventLoop(): void {
  _runMainEventLoop()
}

// Drains all currently-ready main-loop work without blocking.
// This is intended for OS-owned hosts that need to integrate std/event work
// into another event loop.
export function drainMainEventLoop(): int {
  return _drainMainEventLoop()
}

export function setMainEventWakeHandler(handler: (): void): void {
  _setMainEventWakeHandler(handler)
}

export function clearMainEventWakeHandler(): void {
  _clearMainEventWakeHandler()
}
