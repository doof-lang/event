// Event delivery primitives for Doof programs.
//
// `AsyncEventChannel<T>` is conceptually immutable from Doof's point of view:
// the public wrapper contains only readonly fields, while the mutable queue and
// wakeup machinery live inside native code.

import { Duration } from "std/time"

import class NativeAsyncEventChannel from "native_event.hpp" as doof_event::NativeAsyncEventChannel {
  static create(capacity: int, keepsAlive: bool): NativeAsyncEventChannel
  trySend(task: (): void): int
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

export enum AsyncEventChannelError {
  Full,
  Closed,
}

export class AsyncEventChannel<T> {
  private readonly native: NativeAsyncEventChannel
  private readonly handler: (value: T): void

  send(value: T): Result<void, AsyncEventChannelError> {
    code := this.native.trySend((): void => this.handler(value))
    return case code {
      0 -> Success {},
      1 -> Failure { error: AsyncEventChannelError.Full },
      _ -> Failure { error: AsyncEventChannelError.Closed },
    }
  }

  close(): Result<void, AsyncEventChannelError> {
    if this.native.tryClose() {
      return Success {}
    }
    return Failure { error: AsyncEventChannelError.Closed }
  }
}

export function createMainAsyncEventChannel<T>(
  handler: (it: T): void,
  capacity: int = 1024,
  keepsAlive: bool = true,
): AsyncEventChannel<T> {
  if capacity <= 0 {
    panic("AsyncEventChannel capacity must be positive")
  }

  return AsyncEventChannel<T>(
    NativeAsyncEventChannel.create(capacity, keepsAlive),
    handler,
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
  handler: (): void,
  keepsAlive: bool = true,
): Timer {
  if delay.isNegative() {
    panic("setTimeout delay must not be negative")
  }

  return Timer(NativeTimer.createTimeout(delay.toNanos(), keepsAlive, handler))
}

export function setInterval(
  interval: Duration,
  handler: (): void,
  keepsAlive: bool = true,
): Timer {
  if interval.toNanos() <= 0L {
    panic("setInterval interval must be positive")
  }

  return Timer(NativeTimer.createInterval(interval.toNanos(), keepsAlive, handler))
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
