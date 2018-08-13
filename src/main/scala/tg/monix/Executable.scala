package tg.monix

import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, PrivateKey}
import java.time.Instant
import java.util.Base64

import cats.Show
import cats.effect.IO
import monix.eval.{Task, TaskCircuitBreaker}
import monix.reactive.{Consumer, Observable}

import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import monix.execution.misc.NonFatal
import org.http4s.Uri.{Authority, RegName, Scheme}
import org.http4s.{AuthScheme, Credentials, Headers, Method, Request, Response, Uri, UrlForm}
import org.http4s.client._
import org.http4s.client.blaze.Http1Client
import org.http4s.client.dsl.io._
import cats.implicits._
import org.http4s.headers.Authorization

//TODO - looks like unlimited restarting in case CB is not a good idea
object Executable extends App {

  val source = GoogleSub.messages
  val stream = source >>= handle >>= GoogleSub.acknowledgeBatch

  private val esCb = TaskCircuitBreaker(
    maxFailures = 5,
    resetTimeout = 10.seconds,
    exponentialBackoffFactor = 2,
    maxResetTimeout = 10.minutes
  ).memoize

  stream.consumeWith(GoogleSub.acknowledge).runAsync//.cancel()

  while(true) {}

  def handle(message: ReceivedMessage): Observable[Seq[AckId]] =
    Observable.fromTask {
      esCb >>= (_.protect(Task.raiseError(new RuntimeException("Error while processing in business component"))).onError {
        case NonFatal(t) ⇒ GoogleSub.log(t.toString)
      })
    }.onErrorRestartUnlimited
//    Observable(message).map(_.ackId).bufferTimedAndCounted(10.second, 10)
}
//TODO - metrics (number of messages) maybe using pipes ?

trait Time {
  val now: Task[EpochSeconds] = Task.eval(Instant.now().getEpochSecond).map(EpochSeconds.apply)
}

object Time extends Time

case class CircuitBreakerConfig(maxFailures: Int)
case class GoogleSubConfig(projectId: String, clientEmail: String, privateKeyRaw: String, subscription: String, circuitBreakerConfig: CircuitBreakerConfig)

case class GoogleSub(httpClient: Client[IO], config: GoogleSubConfig) {

}

object GoogleSub {
  implicit val showThroable: Show[Throwable] = Show.fromToString[Throwable]

  val host = "localhost"
  val port = 8080
  val uri = Uri(Some(Scheme.http), Some(Authority(None, RegName(host), Some(port))))

  val clientEmail = "email"
  val project = "p1"
  val subscription = "sub1"
  val privateKey: PrivateKey = {
    val kf        = KeyFactory.getInstance("RSA")
    val encodedPv = Base64.getDecoder.decode("private-key")
    val keySpecPv = new PKCS8EncodedKeySpec(encodedPv)
    kf.generatePrivate(keySpecPv)
  }

  val httpClient: Client[IO] = Http1Client[IO]().unsafeRunSync()

  def log(message: String): Task[Unit] = Task.eval(println(message))

  private val circuitBreaker = TaskCircuitBreaker(
    maxFailures = 5,
    resetTimeout = 10.seconds,
    exponentialBackoffFactor = 2,
    maxResetTimeout = 10.minutes
  ).memoize

  private val newToken: Task[AccessToken] = circuitBreaker >>= newTokenWithCircuitBreaker

  private val token: Task[AccessToken] = {
    var maybeToken: Option[AccessToken] = None
    //TODO - consider token failures as well
    for {
      now   ← Time.now
      token ← maybeToken.filter(_.expiresAt >= now).map(Task.now).getOrElse(newToken)
      _     ← Task.eval(maybeToken = Some(token))
    } yield token
  }

  val acknowledge: Consumer[AcknowledgeRequest, Unit] = Consumer.foreachParallelTask(1) { request ⇒
    (token >>= acknowledge(request)).flatMap {
      case Ack ⇒ log(s"Ack: $request")
      case _   ⇒ log("Ack Error")
    }
  }

  def acknowledgeBatch(ackIds: Seq[AckId]): Observable[AcknowledgeRequest] = Observable(ackIds).map(AcknowledgeRequest.apply)

  def acknowledgeBatch(ackId: AckId): Observable[AcknowledgeRequest] =
    Observable(ackId).bufferTimedAndCounted(1.second, 100) >>= acknowledgeBatch

  val messages: Observable[ReceivedMessage] = {
    Observable.interval(2.seconds) >> {
      Observable.fromTask(token >>= GoogleSub.pull).map(_.receivedMessages.getOrElse(Seq.empty)).filter(_.nonEmpty) >>=
      Observable.fromIterable
    }.onErrorRestartUnlimited
  }

  def pull(token: AccessToken): Task[PullResponse] = circuitBreaker >>= pullWithCircuitBreaker(token)

  def acknowledge(request: AcknowledgeRequest)(token: AccessToken): Task[AcknowledgeResponse] = circuitBreaker >>= acknowledgeWithCircuitBreaker(token: AccessToken, request: AcknowledgeRequest)

  private def acknowledgeRequest(request: AcknowledgeRequest): IO[Request[IO]] =
    Method.POST(
      uri.withPath("/v1/projects/" |+| project |+| "/subscriptions/" |+| subscription |+| ":acknowledge"),
      request
    )

  private def acknowledgeWithCircuitBreaker(token: AccessToken, request: AcknowledgeRequest)(cb: TaskCircuitBreaker): Task[AcknowledgeResponse] = cb.protect {
    log("Acknowledging") >>
      Task.fromIO(acknowledgeRequest(request) >>= secureWith(token) >>= (httpClient.fetch[AcknowledgeResponse](_)(toAckResponse))).onError {
        case NonFatal(t) ⇒ log("Failed while acknowledging messages: " |+| t.show)
      }
  }

  private def toAckResponse(response: Response[IO]): IO[AcknowledgeResponse] = response.status.code match {
      case 200 ⇒ IO(Ack)
      case _ ⇒ response.as[String].map(Decline.apply)
    }

  private val pullRequest: IO[Request[IO]] =
    Method.POST(
      uri.withPath("/v1/projects/" |+| project |+| "/subscriptions/" |+| subscription |+| ":pull"),
      PullRequest(true, 1000)
    )

  //TODO - should recover from other than 500 and 401 errors. i.e should open on 500 and 401
  private def pullWithCircuitBreaker(token: AccessToken)(cb: TaskCircuitBreaker): Task[PullResponse] = cb.protect {
      log("Fetching new messages") >>
      Task.fromIO(pullRequest >>= secureWith(token) >>= httpClient.expect[PullResponse]).onError {
        case NonFatal(t) ⇒ log("Failed while pulling messages: " |+| t.show)
      }
  }

  private def secureWith(token: AccessToken)(requet: Request[IO]) = IO {
    requet.withHeaders(Headers(Authorization(Credentials.Token(AuthScheme.Bearer, token.token))))
  }

  private def tokenRequest(request: OAuthRequest): IO[Request[IO]] = IO(request.encode(privateKey)).flatMap { encodedRequest ⇒
    Method.POST(
      uri.withPath("/oauth2/v4/token"),
      UrlForm(
        "grant_type" → "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion" → encodedRequest
      )
    )
  }

  private def newTokenWithCircuitBreaker(cb: TaskCircuitBreaker): Task[AccessToken] = cb.protect {
    for {
      _ ← log("fetching new token")
      now ← Time.now
      expiresAt = now + 3600
      oauthResponse ← Task.fromIO(tokenRequest(OAuthRequest.from(clientEmail, now, expiresAt)) >>= httpClient.expect[OAuthResponse])
    } yield AccessToken(oauthResponse.access_token, expiresAt)
  }.onError {
    case NonFatal(t) ⇒ log("Failed while fetching token: " |+| t.show)
  }

}
