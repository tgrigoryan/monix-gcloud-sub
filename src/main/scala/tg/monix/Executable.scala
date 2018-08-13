package tg.monix

import java.security.KeyPairGenerator

import cats.effect.IO
import monix.reactive.Observable

import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import org.http4s.Uri.{Authority, RegName, Scheme}
import org.http4s.Uri
import org.http4s.client._
import org.http4s.client.blaze.Http1Client
import cats.implicits._
import tg.monix.google._
import tg.monix.logging.Logger
import tg.monix.time.Time

//TODO - looks like unlimited restarting in case CB is not a good idea
//TODO - metrics (number of messages) maybe using pipes ?
object Executable extends App {

  val googleSubConfig = GoogleSubConfig(
    2.second,
    AcknowledgeBatchBufferConfig(1.second, 100)
  )
  val googleSubHttpClientConfig = GoogleSubHttpClientConfig(
    Uri(Some(Scheme.http), Some(Authority(None, RegName("localhost"), Some(8080)))),
    "p1",
    "s1",
    1000,
    ClientConfig(
      "email",
      keyPair.getPrivate,
      3600.seconds
    ),
    CircuitBreakerConfig(5, 10.seconds, 2, 10.minutes)
  )

  val httpClient: Client[IO] = Http1Client[IO]().unsafeRunSync()
  val googleSubClient = GoogleSubHttpClient(httpClient, Time, Logger.console, googleSubHttpClientConfig)
  val googleSub = GoogleSub(googleSubClient, Logger.console, googleSubConfig)

  val source = googleSub.messages
  val stream = googleSub.messages >>= handle >>= googleSub.acknowledgeBatch

  val cf = stream.consumeWith(googleSub.acknowledge).runAsync

  while(!cf.isCompleted) {}

  def handle(message: ReceivedMessage): Observable[Seq[AckId]] =
    Observable(message).map(_.ackId).bufferTimedAndCounted(10.second, 10)

  def keyPair() =
    KeyPairGenerator.getInstance("RSA").generateKeyPair()
}

