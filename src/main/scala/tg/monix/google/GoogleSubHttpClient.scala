package tg.monix.google

import java.security.PrivateKey

import cats.effect.IO
import monix.eval.{Task, TaskCircuitBreaker}

import scala.concurrent.duration._
import monix.execution.misc.NonFatal
import org.http4s.{AuthScheme, Credentials, Headers, Method, Request, Response, Uri, UrlForm}
import org.http4s.client._
import org.http4s.client.dsl.io._
import cats.implicits._
import org.http4s.headers.Authorization
import tg.monix.logging.Logger
import tg.monix.implicits._
import tg.monix.time.Time

trait GoogleSubClient {
  val token: Task[AccessToken]
  def pull(token: AccessToken): Task[PullResponse] //TODO -  Maybe move token management totally inside ?
  def acknowledge(request: AcknowledgeRequest)(token: AccessToken): Task[AcknowledgeResponse]
}

case class CircuitBreakerConfig(
    maxFailures: Int,
    resetTimeout: FiniteDuration,
    exponentialBackoffFactor: Double,
    maxResetTimeout: FiniteDuration
) {
  def create: Task[TaskCircuitBreaker] = TaskCircuitBreaker(
    maxFailures,
    resetTimeout,
    exponentialBackoffFactor,
    maxResetTimeout
  )
}
case class ClientConfig(email: String, privateKey: PrivateKey, sessionDuration: FiniteDuration)
case class GoogleSubHttpClientConfig(
    uri: Uri,
    project: String,
    subscription: String,
    pullMaxMessages: Int,
    client: ClientConfig,
    circuitBreaker: CircuitBreakerConfig
)

//TODO - F[_] could be used instead of IO
//TODO - pass the CB
case class GoogleSubHttpClient(httpClient: Client[IO], time: Time, logger: Logger, config: GoogleSubHttpClientConfig)
    extends GoogleSubClient {

  private val circuitBreaker: Task[TaskCircuitBreaker] = config.circuitBreaker.create.memoize

  private val newToken: Task[AccessToken] = circuitBreaker >>= newTokenWithCircuitBreaker

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  val token: Task[AccessToken] = {
    var maybeToken: Option[AccessToken] = None
    //TODO - consider token failures as well
    for {
      now   ← time.now
      token ← maybeToken.filter(_.expiresAt >= now).map(Task.now).getOrElse(newToken)
      _     ← Task.eval(maybeToken = Some(token))
    } yield token
  }

  def pull(token: AccessToken): Task[PullResponse] = circuitBreaker >>= pullWithCircuitBreaker(token)

  def acknowledge(request: AcknowledgeRequest)(token: AccessToken): Task[AcknowledgeResponse] =
    circuitBreaker >>= acknowledgeWithCircuitBreaker(token: AccessToken, request: AcknowledgeRequest)

  private def acknowledgeRequest(request: AcknowledgeRequest): IO[Request[IO]] =
    Method.POST(
      config.uri
        .withPath("/v1/projects/" |+| config.project |+| "/subscriptions/" |+| config.subscription |+| ":acknowledge"),
      request
    )

  private def acknowledgeWithCircuitBreaker(token: AccessToken, request: AcknowledgeRequest)(
      cb: TaskCircuitBreaker
  ): Task[AcknowledgeResponse] = cb.protect {
    logger.log("Acknowledging") >>
    Task
      .fromIO(
        acknowledgeRequest(request) >>= secureWith(token) >>= (httpClient.fetch[AcknowledgeResponse](_)(toAckResponse))
      )
      .onError {
        case NonFatal(t) ⇒ logger.log("Failed while acknowledging messages: " |+| t.show)
      }
  }

  private def toAckResponse(response: Response[IO]): IO[AcknowledgeResponse] = response.status.code match {
    case 200 ⇒ IO(Ack)
    case _   ⇒ response.as[String].map(Decline.apply)
  }

  private def tokenRequest(request: OAuthRequest): IO[Request[IO]] =
    IO(request.encode(config.client.privateKey)).flatMap { encodedRequest ⇒
      Method.POST(
        config.uri.withPath("/oauth2/v4/token"),
        UrlForm(
          "grant_type" → "urn:ietf:params:oauth:grant-type:jwt-bearer",
          "assertion"  → encodedRequest
        )
      )
    }

  private def newTokenWithCircuitBreaker(cb: TaskCircuitBreaker): Task[AccessToken] =
    cb.protect {
        for {
          _         ← logger.log("fetching new token")
          now       ← time.now
          expiresAt = now + config.client.sessionDuration.toSeconds
          oauthResponse ← Task.fromIO(
                           tokenRequest(OAuthRequest.from(config.client.email, now, expiresAt)) >>= httpClient
                             .expect[OAuthResponse]
                         )
        } yield AccessToken(oauthResponse.access_token, expiresAt)
      }
      .onError {
        case NonFatal(t) ⇒ logger.log("Failed while fetching token: " |+| t.show)
      }

  private val pullRequest: IO[Request[IO]] =
    Method.POST(
      config.uri.withPath("/v1/projects/" |+| config.project |+| "/subscriptions/" |+| config.subscription |+| ":pull"),
      PullRequest(returnImmediately = true, config.pullMaxMessages)
    )

  //TODO - should recover from other than 500 and 401 errors. i.e should open on 500 and 401
  private def pullWithCircuitBreaker(token: AccessToken)(cb: TaskCircuitBreaker): Task[PullResponse] = cb.protect {
    logger.log("Fetching new messages") >>
    Task.fromIO(pullRequest >>= secureWith(token) >>= httpClient.expect[PullResponse]).onError {
      case NonFatal(t) ⇒ logger.log("Failed while pulling messages: " |+| t.show)
    }
  }

  private def secureWith(token: AccessToken)(requet: Request[IO]) = IO {
    requet.withHeaders(Headers(Authorization(Credentials.Token(AuthScheme.Bearer, token.token))))
  }
}
