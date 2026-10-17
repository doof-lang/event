# std/event Guide and API Reference

`std/event` provides the root event-loop hooks and timers used by Doof hosts
and libraries.

Moving work between actors is not part of this module. Every function value is
an actor-affine callback: `callback.post(...)` queues a call on the actor that
created it, and `callback.tryPost(...)` does the same without waiting for
mailbox capacity. Native callbacks deliver into Doof the same way.

## Mental Model

`std/event` has three moving parts:

- **The main event loop** dispatches work queued on the root application actor:
  posted callbacks, root timers, and events from native sources such as servers
  and sockets. Applications call `runMainEventLoop()`; native or UI hosts
  integrate with `drainMainEventLoop()`.
- **Keep-alive sources** decide when `runMainEventLoop()` returns. Timers,
  `MainEventLoopHold`s, and open native sources such as an HTTP server or a
  WebSocket connection each keep the loop running.
- **Timers** schedule callbacks after a delay or at an interval on the actor
  that created them.

## Quick Start

```doof
import { runMainEventLoop, setTimeout } from "std/event"
import { Duration } from "std/time"

class Worker {
  work(report: (message: string): none): none {
    // Runs on the worker actor; `report` runs back on the root actor.
    report.post("done on the worker")
  }
}

function main(): int {
  worker := Actor<Worker>()
  worker.work((message: string): none => println(message))

  timer := setTimeout{
    delay: Duration.ofMillis(100L),
    handler: (): none => println("later"),
  }

  runMainEventLoop()
  return 0
}
```

## Delivering Work Without Channels

A producer that must not block uses `tryPost`. It fails with `"full"` when the
owner's mailbox is at capacity and `"retired"` when the owner is gone; nothing
is queued on failure.

Per-producer backpressure counts calls that have been posted but not yet
handled. The promise from `tryPost` completes once the handler has run, so it
is the natural place to resume:

```doof
class Producer {
  let inFlight = 0
  let next = 0
  readonly limit = 64
  handle: (value: int): none

  pump(): none {
    while this.inFlight < this.limit {
      case this.handle.tryPost(this.next) {
        s: Success -> {
          this.inFlight += 1
          this.next += 1
          s.value.onComplete((result) => {
            this.inFlight -= 1
            this.pump()
          })
        }
        _: Failure -> return
      }
    }
  }
}
```

To coalesce "latest value wins" updates such as progress or redraw requests,
keep the pending value on the producer and post a single flush that reads it.

## Event Loop Integration

### `runMainEventLoop()`

```doof
runMainEventLoop()
```

Blocks the calling thread and dispatches root work until both of these are
true:

- no keep-alive sources remain
- the root mailbox has drained

This is the normal entry point for simple programs that use `std/event`
directly.

### `retainMainEventLoop()`

```doof
hold := retainMainEventLoop()
// ...later, once the awaited event has been handled:
hold.release()
```

Keeps `runMainEventLoop()` running until the hold is released or dropped. Use it
when the next event comes from somewhere the loop cannot see, such as a reply
that will be posted from another thread. `release()` returns `true` only for the
call that released the hold.

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
setMainEventWakeHandler((): none => {
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
  handler: (): none => println("later"),
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
  handler: (): none => println("tick"),
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

### Event Loop

```doof
runMainEventLoop(): none
drainMainEventLoop(): int
setMainEventWakeHandler(handler: (): none): none
clearMainEventWakeHandler(): none

class MainEventLoopHold {
  release(): bool
}

retainMainEventLoop(): MainEventLoopHold
```

### Timers

```doof
class Timer {
  cancel(): bool
}

setTimeout(
  delay: Duration,
  handler: (): none,
  keepsAlive: bool = true,
): Timer

setInterval(
  interval: Duration,
  handler: (): none,
  keepsAlive: bool = true,
): Timer
```
