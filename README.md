# std/event

`std/event` provides the root event loop and timers for Doof programs: explicit
main-loop pumping hooks, a hold that keeps the loop running, and scheduled
timers.

Handing work to another actor needs no channel. Every function value belongs to
the actor that created it, and `callback.post(...)` or the non-blocking
`callback.tryPost(...)` queues a call there. Native sources such as
`std/http-server` and `std/http` WebSockets deliver events the same way.

## Documentation

- [Guide and API reference](docs/API.md) explains the event loop, keep-alive
  sources, cross-actor delivery and backpressure with `tryPost`, timers, and
  lifecycle rules.
- Tests can be run with `doof test event`.

## Usage

```doof
import { runMainEventLoop, setTimeout } from "std/event"
import { Duration } from "std/time"

function main(): int {
  report := (message: string): none => println(message)
  report.post("hello from the event queue")

  timer := setTimeout{
    delay: Duration.ofMillis(100L),
    handler: (): none => println("hello from a timer"),
  }

  runMainEventLoop()
  return 0
}
```

## Exports

### `runMainEventLoop()`

Blocks efficiently on the calling thread, dispatching queued root work until no
keep-alive sources (timers, holds, open servers and connections) remain and the
root mailbox has drained.

This is the first-cut explicit host hook. Longer term, ordinary applications
should not need to expose an event-loop concept directly; generated hosts can
call the same runtime hook themselves.

### `retainMainEventLoop()`

```doof
hold := retainMainEventLoop()
hold.release()
```

Keeps `runMainEventLoop()` running until the returned `MainEventLoopHold` is
released or dropped. `release()` returns `true` only for the call that released
the hold.

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
  handler: (): none => ...,
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
  handler: (): none => println("tick"),
}

canceled := timer.cancel()
```

Schedules `handler` repeatedly on the main event loop. Intervals must be
positive. Recurring timers do not catch up missed ticks; the next interval is
scheduled after the current callback has been delivered.
