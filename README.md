# std/event

`std/event` provides the first event-delivery primitive for Doof programs:
`AsyncEventChannel<T>`.

An async event channel accepts immutable values from producers and delivers them
serially to a handler on the owning application thread. The Doof wrapper is
conceptually immutable; mutable queue state and wakeup bookkeeping are held in
native code.

## Usage

```doof
import { createMainAsyncEventChannel, runMainEventLoop } from "std/event"

function main(): int {
  events := createMainAsyncEventChannel{
    handler: (message: string): void => println(message),
    capacity: 256,
    keepsAlive: false,
  }

  try! events.send("hello from the event queue")
  runMainEventLoop()
  return 0
}
```

## Exports

### `AsyncEventChannel<T>`

```doof
send(value: T): Result<void, AsyncEventChannelError>
close(): Result<void, AsyncEventChannelError>
```

`send(...)` is nonblocking. It returns:

- `Success {}` when the value was accepted;
- `Failure { error: .Full }` when the bounded queue is at capacity;
- `Failure { error: .Closed }` after the channel has been closed.

### `createMainAsyncEventChannel{ ... }`

```doof
createMainAsyncEventChannel{
  handler: (value: T): void => ...,
  capacity: 1024,
  keepsAlive: true,
}
```

Creates a channel whose handler is dispatched by the main application event
pump. Capacity must be positive.

### `runMainEventLoop()`

Blocks efficiently on the calling thread, dispatching queued handlers until no
keep-alive channels remain open and the ready queue has drained.

This is the first-cut explicit host hook. Longer term, ordinary applications
should not need to expose an event-loop concept directly; generated hosts can
call the same runtime seam themselves.
