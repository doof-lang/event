// Event delivery primitives for Doof programs.
//
// `AsyncEventChannel<T>` is conceptually immutable from Doof's point of view:
// the public wrapper contains only readonly fields, while the mutable queue and
// wakeup machinery live inside native code.

import class NativeAsyncEventChannel from "native_event.hpp" as doof_event::NativeAsyncEventChannel {
  static create(capacity: int, keepsAlive: bool): NativeAsyncEventChannel
  trySend(task: (): void): int
  tryClose(): bool
}

import function _runMainEventLoop(): void from "native_event.hpp" as doof_event::runMainEventLoop

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
  handler: (value: T): void,
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

// First-cut explicit pump for hosts that do not yet have runtime integration.
// It blocks efficiently while keep-alive channels remain open, dispatches
// channel handlers on the calling thread, and returns once no keep-alive
// channels remain and the ready queue has drained.
export function runMainEventLoop(): void {
  _runMainEventLoop()
}
