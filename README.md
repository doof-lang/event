# std/event

`std/event` provides event-delivery primitives for Doof programs:
`AsyncEventChannel<T>` and scheduled timers.

An async event channel accepts immutable values from producers and delivers them
serially to a handler on the owning application thread. The Doof wrapper is
conceptually immutable; mutable queue state and wakeup bookkeeping are held in
native code.

## Usage

```doof
import { createMainAsyncEventChannel, runMainEventLoop, setTimeout } from "std/event"
import { Duration } from "std/time"

function main(): int {
  events := createMainAsyncEventChannel{
    handler: (message: string): void => println(message),
    capacity: 256,
    keepsAlive: false,
  }

  timer := setTimeout{
    delay: Duration.ofMillis(100L),
    handler: (): void => try! events.send("hello from a timer"),
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

### `Timer`

```doof
cancel(): bool
```

`cancel()` returns `true` only when it actually cancels an active timer and
prevents a future timer callback. It returns `false` if the timer was already
canceled, already completed, or in the case of a one-shot timer, already
committed to dispatch.

### `setTimeout{ ... }`

```doof
setTimeout{
  delay: Duration.ofMillis(250L),
  handler: (): void => ...,
  keepsAlive: true,
}
```

Schedules `handler` to run once on the main event loop after `delay`.
`Duration.ZERO` schedules the callback for a future event-loop turn. Negative
delays panic.

Timers keep the event loop alive by default. Pass `keepsAlive: false` for a
passive timer that can run while some other source keeps the loop draining, but
does not keep the loop alive by itself.

### `setInterval{ ... }`

```doof
timer := setInterval{
  interval: Duration.ofSeconds(1L),
  handler: (): void => println("tick"),
}

canceled := timer.cancel()
```

Schedules `handler` repeatedly on the main event loop. Intervals must be
positive. Recurring timers do not catch up missed ticks; the next interval is
scheduled after the current callback has been delivered.
