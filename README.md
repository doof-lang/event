# std/event

`std/event` provides event-delivery primitives for Doof programs:
`Channel<T>` and scheduled timers.

Channels accept immutable values from producers and deliver them serially to a
handler on the owning application thread. Mutable queue state and wakeup
bookkeeping are held in native code.

## Usage

```doof
import { ChannelClosed, ChannelMessage, ChannelReady, createChannel, runMainEventLoop, setTimeout } from "std/event"
import { Duration } from "std/time"

function main(): int {
  events := createChannel{
    handler: (event: ChannelMessage<string> | ChannelReady<string> | ChannelClosed<string>): void => {
      case event {
        message: ChannelMessage<string> -> println(message.value)
        _: ChannelReady<string> -> {}
        _: ChannelClosed<string> -> {}
      }
    },
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

### `Channel<T>`

```doof
enum Backpressure { None, High }
enum SendError { Full, Closed }

class ChannelMessage<T> { readonly value: T }
class ChannelReady<T> {}
class ChannelClosed<T> {}

createChannel{ ... }: Channel<T>
send(value: T, key: string | null = null): Result<Backpressure, SendError>
close(): void
```

`Channel<T>` is a bounded, nonblocking ingress point for bidirectional event
sources such as native websocket integrations and future cross-thread
communication. Values sent through channels are intended to be immutable when
they cross native or thread boundaries; the current compiler does not yet expose
an `Immutable` generic constraint, so this is documented as an API contract
rather than encoded in the type parameter.

```doof
events := createChannel{
  capacity: 256,
  highWater: 192,
  lowWater: 128,
  handler: (event: ChannelMessage<string> | ChannelReady<string> | ChannelClosed<string>): void => {
    case event {
      message: ChannelMessage<string> -> println(message.value),
      _: ChannelReady<string> -> println("ready for more"),
      _: ChannelClosed<string> -> println("closed"),
    }
  },
}
```

`send(...)` returns `Backpressure.None` while the queue remains below
`highWater`, and `Backpressure.High` once the queued message count reaches or
exceeds `highWater`. If the queue is already at capacity, it fails with
`SendError.Full`; after `close()`, it fails with `SendError.Closed`.

When a non-null `key` is supplied, a pending message with the same key is
replaced in place instead of consuming another capacity slot. The original FIFO
position for that key is preserved. Unkeyed messages are always appended.

After high backpressure has been reported, the handler receives one
`ChannelReady` event when dispatch lowers queued depth to `lowWater` or below.
`close()` stops accepting new messages immediately, drains pending messages, and
then delivers one `ChannelClosed` event. Repeated `close()` calls are no-ops.

When omitted, `highWater` defaults to the channel capacity and `lowWater`
defaults to half of the effective `highWater`.

### `runMainEventLoop()`

Blocks efficiently on the calling thread, dispatching queued handlers until no
keep-alive channels remain open and the ready queue has drained.

This is the first-cut explicit host hook. Longer term, ordinary applications
should not need to expose an event-loop concept directly; generated hosts can
call the same runtime seam themselves.

### `drainMainEventLoop()`

```doof
count := drainMainEventLoop()
```

Dispatches all currently-ready main-loop work without blocking and returns the
number of handlers that ran. This is primarily for native hosts that own an OS
event loop, such as a windowing or game runtime, and need to integrate
`std/event` work into that loop.

### `setMainEventWakeHandler(...)` / `clearMainEventWakeHandler()`

Installs or clears a host wake callback. The callback is invoked when new
main-loop work becomes ready, so an OS-owned event loop can post back to its
main thread and call `drainMainEventLoop()`. Ordinary applications should not
need these functions directly.

The root event mailbox does not own an operating-system thread. Hosts that have
strict thread-affinity requirements, such as UI runtimes, should arrange for
`drainMainEventLoop()` to run only on the thread they own.

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
