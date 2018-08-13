package tg.monix.google

import java.security.{PrivateKey, Signature}
import java.time.Instant
import java.util.Base64

import cats.effect.IO
import io.circe.syntax._
import io.circe._
import io.circe.generic.semiauto._
import org.http4s._
import org.http4s.circe._
import io.circe.java8.time._
import tg.monix.time.EpochSeconds
import cats.implicits._

case class PubSubMessage(
    data: String,
    messageId: String,
    attributes: Option[Map[String, String]] = None,
    publishTime: Option[Instant] = None
)

object PubSubMessage {
  implicit val decoder = deriveDecoder[PubSubMessage]
}

case class AckId(id: String) extends AnyVal
case class ReceivedMessage(ackId: AckId, message: PubSubMessage)
object ReceiveMessage {
  val decoder: Decoder[ReceivedMessage] = (c: HCursor) =>
    for {
      ackId   <- c.downField("ackId").as[String]
      message <- c.downField("message").as[PubSubMessage]
    } yield ReceivedMessage(AckId(ackId), message)
}

case class AcknowledgeRequest(ackIds: Seq[AckId])
object AcknowledgeRequest {
  implicit val encoderAckId: Encoder[AckId]                         = Encoder.instance[AckId](_.id.asJson)
  implicit val encoder: Encoder[AcknowledgeRequest]                 = deriveEncoder[AcknowledgeRequest]
  implicit val entityEncoder: EntityEncoder[IO, AcknowledgeRequest] = jsonEncoderOf[IO, AcknowledgeRequest]
}

sealed trait AcknowledgeResponse
case object Ack                  extends AcknowledgeResponse
case class Decline(body: String) extends AcknowledgeResponse

case class PullRequest(returnImmediately: Boolean, maxMessages: Int)
object PullRequest {
  implicit val encoder: Encoder[PullRequest]                 = deriveEncoder[PullRequest]
  implicit val entityEncoder: EntityEncoder[IO, PullRequest] = jsonEncoderOf[IO, PullRequest]
}

case class PullResponse(receivedMessages: Option[Seq[ReceivedMessage]])
object PullResponse {
  implicit val decoderAckId: Decoder[AckId]                     = deriveDecoder[AckId]
  implicit val decoderPubSubMessage: Decoder[PubSubMessage]     = deriveDecoder[PubSubMessage]
  implicit val decoderReceivedMessage: Decoder[ReceivedMessage] = ReceiveMessage.decoder
  implicit val decoderPullResponse: Decoder[PullResponse]       = deriveDecoder[PullResponse]
  implicit val entityDecoder: EntityDecoder[IO, PullResponse]   = jsonOf[IO, PullResponse]
}

case class OAuthRequest(iss: String, scope: String, aud: String, exp: Long, iat: Long) { self ⇒

  val header  = base64(OAuthRequest.rawHeader.getBytes("UTF-8"))
  val payload = base64(OAuthRequest.encoder(self).noSpaces.getBytes("UTF-8"))
  val request = header |+| "." |+| payload

  def signedRequest(privateKey: PrivateKey): String = {
    val signature = Signature.getInstance("SHA256withRSA")
    signature.initSign(privateKey)
    signature.update(request.getBytes("UTF-8"))

    request |+| "." |+| base64(signature.sign())
  }

  private def base64(s: Array[Byte]) = Base64.getUrlEncoder.encodeToString(s)
}

object OAuthRequest {

  val rawHeader = """{"alg":"RS256","typ":"JWT"}"""

  def from(clientEmail: String, now: EpochSeconds, expiresAt: EpochSeconds): OAuthRequest = OAuthRequest(
    clientEmail,
    "https://www.googleapis.com/auth/pubsub",
    "https://www.googleapis.com/oauth2/v4/token",
    expiresAt.seconds,
    now.seconds
  )
  private val encoder: Encoder[OAuthRequest] = deriveEncoder[OAuthRequest]
}
case class OAuthResponse(access_token: String, token_type: String, expires_in: Option[Int])
object OAuthResponse {
  implicit val decoder: Decoder[OAuthResponse]                 = deriveDecoder[OAuthResponse]
  implicit val entityDecoder: EntityDecoder[IO, OAuthResponse] = jsonOf[IO, OAuthResponse]
}

case class AccessToken(token: String, expiresAt: EpochSeconds)
