import {
  ChannelReceiver,
  ChannelSender,
  createChannel,
  runMainEventLoop
} from "std/event"
import { Duration, Instant, Thread } from "std/time"

function timestamp(): string => Instant.now().toISOString()

function log(source: string, message: string): none {
  println("[${timestamp()}] ${source}: ${message}")
}

class ConsoleActor {
  sleep(): none {
    log("actor", "sleeping for 5 seconds")
    Thread.sleep(Duration.ofSeconds(5L))
    log("actor", "woke up")
  }
  attachReceiver(receiver: ChannelReceiver<string>): none {
    receiver.onMessage((message: string): none => log("actor", message))
    receiver.onClosed((): none => log("actor", "channel closed"))
  }
}

class Producer {
  let idx = 0
  sender: ChannelSender<string>
  totalMessages: int

  pump() {
    while (idx <= totalMessages) {
      sent := sender.send("message ${idx}") else {
        log("producer", "failed to send message ${idx}")
        break
      }
      if sent == .High {
        log("producer", "backpressure applied at message ${idx}")
        break
      }
      idx += 1
    } then {
      log("producer", "finished sending messages")
      sender.close()
    }
  }



}

function main(): int {
  let idx = 0

  actor := Actor<ConsoleActor>()
  (sender, receiver) := createChannel<string>{
    capacity: 16,
    highWater: 12,
    keepsAlive: true,
  }

  actor.attachReceiver(receiver)

  producer := Producer { sender, totalMessages: 4 }
  sender.onReady((): none => producer.pump())
  sender.onClosed((): none => log("main", "channel closed"))
  async actor.sleep()

  log("main", "sending messages")

  producer.pump()

  log("main", "sleeping for 2 seconds")
  Thread.sleep(Duration.ofSeconds(2L))
  log("main", "woke up")
runMainEventLoop()

//  sender.close()

  //retired := retire actor
  return 0
}
