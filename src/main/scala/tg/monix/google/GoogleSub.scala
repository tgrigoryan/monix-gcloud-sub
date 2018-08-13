package tg.monix.google

import monix.reactive.{Consumer, Observable}

import scala.concurrent.duration._
import cats.implicits._
import tg.monix.logging.Logger

case class AcknowledgeBatchBufferConfig(time: FiniteDuration, maxCount: Int)
case class GoogleSubConfig(pullInterval: FiniteDuration, acknowledgeBatchBuffer: AcknowledgeBatchBufferConfig)

case class GoogleSub(client: GoogleSubClient, logger: Logger, config: GoogleSubConfig) {

  val messages: Observable[ReceivedMessage] = {
    Observable.interval(config.pullInterval) >> {
      Observable
        .fromTask(client.token >>= client.pull)
        .map(_.receivedMessages.getOrElse(Seq.empty))
        .filter(_.nonEmpty) >>=
      Observable.fromIterable
    }.onErrorRestartUnlimited
  }

  def acknowledgeBatch(ackIds: Seq[AckId]): Observable[AcknowledgeRequest] =
    Observable(ackIds).map(AcknowledgeRequest.apply)

  def acknowledgeBatch(ackId: AckId): Observable[AcknowledgeRequest] =
    Observable(ackId)
      .bufferTimedAndCounted(config.acknowledgeBatchBuffer.time, config.acknowledgeBatchBuffer.maxCount) >>= acknowledgeBatch

  val acknowledge: Consumer[AcknowledgeRequest, Unit] = Consumer.foreachParallelTask(1) { request ⇒
    (client.token >>= client.acknowledge(request)).flatMap {
      case Ack ⇒ logger.log(s"Ack: $request")
      case _   ⇒ logger.log("Ack Error")
    }
  }
}
