# std/event

`std/event` provides event-delivery primitives for Doof programs: one-way
channels for handing work to an endpoint, explicit main-loop pumping hooks, and
scheduled timers.

Channels are bounded queues split into two endpoint objects. A
`ChannelSender<T>` can enqueue values, observe backpressure, and close the
queue. A `ChannelReceiver<T>` can install the message handler, observe closure,
and close the same queue from the receiving side. The channel itself is not a
bidirectional conversation: values only flow from sender to receiver. If a
protocol needs replies or two independent streams, create a second channel and
pass the opposite endpoints to the participating actors or native integrations.

This endpoint split is useful when ownership matters. A native listener can
hold only the sender for inbound requests, an actor can hold only the receiver
for actor-affine callbacks, and a producer can react to `onReady` without being
able to consume messages.

## Documentation

- [Guide and API reference](docs/API.md) explains channels, backpressure,
  keyed replacement, event-loop integration, timers, and lifecycle rules.
- Tests can be run with `doof test event`.
- [Samples](samples/) show complete programs built with this module.

## Usage

```doof
import { createChannel, runMainEventLoop, setTimeout } from "std/event"
import { Duration } from "std/time"

function main(): int {
  (sender, receiver) := createChannel<string>{
    capacity: 256,
    keepsAlive: false,
  }

  receiver.onMessage((message: string): void => println(message))
  receiver.onClosed((): void => println("receiver closed"))
  sender.onReady((): void => println("ready for more"))
  sender.onClosed((): void => println("sender closed"))

  timer := setTimeout{
    delay: Duration.ofMillis(100L),
    handler: (): void => try! sender.send("hello from a timer"),
  }

  try! sender.send("hello from the event queue")
  sender.close()

  runMainEventLoop()
  return 0
}
```

## Exports

### Channels

```doof
enum Backpressure { None, High }
enum SendError { Full, Closed }

class ChannelSender<T> {
  send(value: T, key: string | null = null): Result<Backpressure, SendError>
  onReady(handler: (): void): void
  onClosed(handler: (): void): void
  close(): void
}

class ChannelReceiver<T> {
  onMessage(handler: (value: T): void): void
  onClosed(handler: (): void): void
  close(): void
}

createChannel{ ... }: Tuple<ChannelSender<T>, ChannelReceiver<T>>
```

Channels are bounded, nonblocking ingress points for handing immutable values to
an endpoint. They are a good fit for native event sources, producer-to-actor
handoff, and future cross-thread communication. Values sent through channels are
intended to be immutable when they cross native or thread boundaries; the
current compiler does not yet expose an `Immutable` generic constraint, so this
is documented as an API contract rather than encoded in the type parameter.

```doof
(sender, receiver) := createChannel<string>{
  capacity: 256,
  highWater: 192,
  lowWater: 128,
}

receiver.onMessage((message: string): void => println(message))
sender.onReady((): void => println("ready for more"))
```

`ChannelSender.send(...)` returns `Backpressure.None` while the queue remains
below `highWater`, and `Backpressure.High` once the queued message count reaches
or exceeds `highWater`. If the queue is already at capacity, it fails with
`SendError.Full`; after `close()`, it fails with `SendError.Closed`.

When a non-null `key` is supplied, a pending message with the same key is
replaced in place instead of consuming another capacity slot. The original FIFO
position for that key is preserved. Unkeyed messages are always appended.

After high backpressure has been reported, the sender receives one `onReady`
callback when dispatch lowers queued depth to `lowWater` or below. Sending
before `receiver.onMessage(...)` is registered is allowed; messages remain
buffered until the receiver endpoint installs its message handler.

`close()` on either endpoint stops accepting new messages immediately. Pending
messages drain to the receiver, then `receiver.onClosed(...)` is delivered.
`sender.onClosed(...)` is delivered to the sender endpoint. Repeated `close()`
calls are no-ops, and ready/closed notifications that occur before their
callback is registered are delivered after registration.

When omitted, `highWater` defaults to the channel capacity and `lowWater`
defaults to half of the effective `highWater`.

### `runMainEventLoop()`

Blocks efficiently on the calling thread, dispatching queued handlers until no
keep-alive channels or timers remain open and the ready queue has drained.

This is the first-cut explicit host hook. Longer term, ordinary applications
should not need to expose an event-loop concept directly; generated hosts can
call the same runtime hook themselves.

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
