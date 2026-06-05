import {
  Channel,
  ChannelClosed,
  ChannelMessage,
  ChannelReady,
  createChannel,
} from "std/event"
import { Duration, Instant, Thread } from "std/time"

function timestamp(): string => Instant.now().toISOString()

function log(source: string, message: string): void {
  println("[${timestamp()}] ${source}: ${message}")
}

class ConsoleActor {
  sleep(): void {
    log("actor", "sleeping for 5 seconds")
    Thread.sleep(Duration.ofSeconds(5L))
    log("actor", "woke up")
  }
  openChannel(): Channel<string> {
    return createChannel<string>{
      handler: (event: ChannelMessage<string> | ChannelReady<string> | ChannelClosed<string>): void => {
        case event {
          message: ChannelMessage<string> -> log("actor", message.value)
          _: ChannelReady<string> -> log("actor", "ready for more")
          _: ChannelClosed<string> -> log("actor", "channel closed")
        }
      },
      capacity: 16,
      highWater: 12,
      keepsAlive: false,
    }
  }
}

function main(): int {
  actor := Actor<ConsoleActor>()
  inbox := actor.openChannel()
  async actor.sleep()

  log("main", "sending messages")
  for idx of 1..20 {
    println(inbox.send("message ${idx}"))
  }

  log("main", "sleeping for 10 seconds")
  Thread.sleep(Duration.ofSeconds(10L))
  log("main", "woke up")

  inbox.close()

  retired := retire actor
  return 0
}
