# std/event Guide and API Reference

`std/event` provides the low-level event-delivery pieces used by Doof hosts and
libraries: bounded one-way channels, explicit main event-loop hooks, and timers.

Use it when some producer needs to hand work to another execution context
without blocking: a native callback delivering into Doof, a background actor
sending work to an actor-owned receiver, or a host event loop waking Doof code on
the main thread.

## Mental Model

`std/event` has three moving parts:

- **Channels** move values from a `ChannelSender<T>` to a
  `ChannelReceiver<T>`. The sender and receiver are separate objects so each
  side can own only the capabilities it needs.
- **The main event loop** dispatches callbacks that belong to the root runtime
  context. Applications can call `runMainEventLoop()`, while native or UI hosts
  can integrate with `drainMainEventLoop()`.
- **Timers** schedule callbacks onto the same event delivery system after a
  delay or at an interval.

Channels are not request/reply streams. If a protocol needs responses, create a
second channel and pass the opposite endpoints to the two participants.

## Quick Start

```doof
import { createChannel, runMainEventLoop, setTimeout } from "std/event"
import { Duration } from "std/time"

function main(): int {
  (sender, receiver) := createChannel<string>{
    capacity: 8,
    highWater: 6,
    lowWater: 3,
    keepsAlive: false,
  }

  receiver.onMessage((message: string): void => println(message))
  receiver.onClosed((): void => println("closed"))

  sender.onReady((): void => println("queue recovered"))

  try! sender.send("queued immediately")

  timer := setTimeout{
    delay: Duration.ofMillis(100L),
    handler: (): void => {
      try! sender.send("queued from timer")
      sender.close()
    },
  }

  runMainEventLoop()
  return 0
}
```

Messages can be sent before `receiver.onMessage(...)` is registered. They remain
buffered until the receiver installs its handler.

## Channels

Create a channel with `createChannel<T>{...}`:

```doof
(sender, receiver) := createChannel<int>{
  capacity: 256,
  highWater: 192,
  lowWater: 128,
  keepsAlive: true,
}
```

Arguments:

- `capacity`: maximum number of pending messages. Must be positive. Defaults to
  `256`.
- `highWater`: queue depth at which `send(...)` starts reporting
  `Backpressure.High`. Defaults to `capacity` when omitted or `0`.
- `lowWater`: queue depth at which the sender is considered ready again after
  high backpressure. Defaults to half of `highWater` when omitted or negative.
- `keepsAlive`: whether the open channel keeps `runMainEventLoop()` alive.
  Defaults to `true`.

`highWater` must be between `1` and `capacity`. `lowWater` must be between `0`
and `highWater`.

### Sending

`send(value)` is nonblocking and returns `Result<Backpressure, SendError>`:

- `Success(Backpressure.None)`: the message was queued below high water.
- `Success(Backpressure.High)`: the message was queued and the queue is now at
  or above high water.
- `Failure(SendError.Full)`: capacity is exhausted.
- `Failure(SendError.Closed)`: either endpoint has closed the channel.

```doof
result := sender.send(value)

case result {
  s: Success -> {
    if s.value == Backpressure.High {
      println("pause producer until onReady")
    }
  },
  f: Failure -> {
    case f.error {
      SendError.Full -> println("queue is full")
      SendError.Closed -> println("channel is closed")
    }
  },
}
```

Values sent through channels are intended to be immutable when they cross native
or thread boundaries. The current compiler does not yet expose an `Immutable`
generic constraint, so this is an API contract rather than something encoded in
`T`.

### Backpressure

Backpressure is cooperative. A channel does not block the sender; it tells the
sender when to slow down.

```doof
class Producer {
  sender: ChannelSender<string>
  next = 0

  pump(): void {
    while true {
      result := sender.send("message ${next}") else { return }
      next = next + 1

      if result == Backpressure.High {
        return
      }
    }
  }
}

producer := Producer { sender }
sender.onReady((): void => producer.pump())
producer.pump()
```

After a send reports `Backpressure.High`, the sender receives one `onReady`
callback when dispatch lowers the queued depth to `lowWater` or below. It will
not receive another ready callback until the queue reaches high water again and
recovers again.

If the queue is full, an unkeyed send fails with `SendError.Full`; no message is
queued.

### Keyed Replacement

`send(value, key)` can coalesce pending work. When `key` is non-null and a
pending message with the same key already exists, the new value replaces the old
value in that same FIFO position instead of consuming another capacity slot.

```doof
try! sender.send(10, "progress")
try! sender.send(20, "progress")
try! sender.send(30, "progress")
```

The receiver sees only `30` for the `"progress"` slot, ordered where the first
`"progress"` message was queued. Unkeyed messages are always appended and never
replace earlier messages.

Keyed replacement is useful for redraw requests, progress updates, status
snapshots, and other "latest value wins" signals.

### Receiving

Install a message handler with `receiver.onMessage(...)`:

```doof
receiver.onMessage((value: int): void => {
  println("received ${value}")
})
```

Handlers run in the context that owns the receiver. If an actor creates or owns
the receiver and registers the handler, delivered messages run on that actor and
preserve the actor mailbox ordering relative to other actor work.

```doof
class Worker {
  attach(receiver: ChannelReceiver<string>): void {
    receiver.onMessage((message: string): void => this.handle(message))
  }

  handle(message: string): void {
    println(message)
  }
}

worker := Actor<Worker>()
(sender, receiver) := createChannel<string>{ keepsAlive: false }
worker.attach(receiver)
try! sender.send("runs on the worker actor")
```

Actor-owned channels do not require `drainMainEventLoop()` for delivery to that
actor. Root callbacks, root-owned receivers, and root timers are delivered by
the main event loop.

### Closing

Either endpoint can close the channel:

```doof
sender.close()
receiver.close()
```

Close behavior:

- New sends fail immediately with `SendError.Closed`.
- Pending messages still drain to the receiver.
- `receiver.onClosed(...)` runs after pending messages have drained.
- `sender.onClosed(...)` runs for the sender endpoint.
- Repeated `close()` calls are no-ops.
- Ready and closed notifications that occur before their handler is registered
  are delivered after registration.

```doof
receiver.onClosed((): void => println("receiver drained and closed"))
sender.onClosed((): void => println("sender closed"))
```

## Event Loop Integration

### `runMainEventLoop()`

```doof
runMainEventLoop()
```

Blocks the calling thread and dispatches ready root work until both of these are
true:

- no keep-alive channels or timers remain open
- the ready queue has drained

This is the normal entry point for simple programs that use `std/event`
directly.

Channels and timers keep the loop alive by default. Pass `keepsAlive: false` for
passive sources that should run only while some other keep-alive source is
present or while a host explicitly drains work.

### `drainMainEventLoop()`

```doof
dispatched := drainMainEventLoop()
```

Runs currently-ready root work without blocking and returns the number of
handlers that ran. It returns `0` when no root work is ready.

Use this when another system owns the application's event loop, such as a UI,
game, or platform host.

### `setMainEventWakeHandler(...)`

```doof
setMainEventWakeHandler((): void => {
  // Post to the host main thread, then call drainMainEventLoop() there.
})

clearMainEventWakeHandler()
```

Installs or clears a wake callback. The runtime invokes the wake callback when
new root work becomes ready, allowing an OS-owned host loop to wake its main
thread and call `drainMainEventLoop()`.

The root event mailbox does not own an operating-system thread. Hosts with
thread-affine APIs must arrange for `drainMainEventLoop()` to run on the thread
they own.

## Timers

Timers schedule callbacks onto the event delivery system. A timer created from
inside an actor dispatches on that actor. A timer created from root dispatches on
the main event loop.

### `setTimeout{...}`

```doof
timer := setTimeout{
  delay: Duration.ofMillis(250L),
  handler: (): void => println("later"),
  keepsAlive: true,
}
```

Runs `handler` once after `delay`.

- `Duration.ZERO` schedules the callback for a future event-loop turn.
- Negative delays panic.
- Timeout timers keep the event loop alive by default.
- Pass `keepsAlive: false` for a passive timeout.

### `setInterval{...}`

```doof
timer := setInterval{
  interval: Duration.ofSeconds(1L),
  handler: (): void => println("tick"),
}
```

Runs `handler` repeatedly. Intervals must be positive.

Recurring timers do not catch up missed ticks. The next interval is scheduled
after the current callback has been delivered.

### `Timer.cancel()`

```doof
canceled := timer.cancel()
```

Returns `true` only when it actually cancels an active timer and prevents a
future callback. It returns `false` if the timer was already canceled, already
completed, or, for a one-shot timer, already committed to dispatch.

## API Map

### Channels

```doof
enum Backpressure {
  None,
  High,
}

enum SendError {
  Full,
  Closed,
}

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

createChannel<T>(
  capacity: int = 256,
  highWater: int = 0,
  lowWater: int = -1,
  keepsAlive: bool = true,
): Tuple<ChannelSender<T>, ChannelReceiver<T> >
```

### Event Loop

```doof
runMainEventLoop(): void
drainMainEventLoop(): int
setMainEventWakeHandler(handler: (): void): void
clearMainEventWakeHandler(): void
```

### Timers

```doof
class Timer {
  cancel(): bool
}

setTimeout(
  delay: Duration,
  handler: (): void,
  keepsAlive: bool = true,
): Timer

setInterval(
  interval: Duration,
  handler: (): void,
  keepsAlive: bool = true,
): Timer
```
