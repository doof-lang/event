import { Assert } from "std/assert"

import { AsyncEventChannelError, createMainAsyncEventChannel, runMainEventLoop } from "../index"

export function testMainAsyncEventChannelDispatchesQueuedValues(): void {
  let handled: int[] = []
  events := createMainAsyncEventChannel{
    handler: (event: int): void => handled.push(event),
    capacity: 4,
    keepsAlive: false,
  }

  try! events.send(1)
  try! events.send(2)
  try! events.send(3)

  runMainEventLoop()

  Assert.equal(handled.length, 3)
  Assert.equal(handled[0], 1)
  Assert.equal(handled[1], 2)
  Assert.equal(handled[2], 3)
}

export function testMainAsyncEventChannelReportsFull(): void {
  events := createMainAsyncEventChannel<int>{
    handler: (event: int): void => {},
    capacity: 1,
    keepsAlive: false,
  }

  try! events.send(1)
  overflow := events.send(2)

  case overflow {
    s: Success -> Assert.fail("expected second send to fail")
    f: Failure -> Assert.equal(f.error, AsyncEventChannelError.Full)
  }

  runMainEventLoop()
}

export function testMainAsyncEventChannelReportsClosed(): void {
  events := createMainAsyncEventChannel<int>{
    handler: (event: int): void => {},
    capacity: 1,
    keepsAlive: false,
  }

  try! events.close()
  afterClose := events.send(1)

  case afterClose {
    s: Success -> Assert.fail("expected send after close to fail")
    f: Failure -> Assert.equal(f.error, AsyncEventChannelError.Closed)
  }
}
