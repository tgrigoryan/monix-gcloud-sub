package tg.monix.google

import java.security.{KeyPairGenerator, PublicKey, Signature}
import java.util.Base64

import io.circe.Decoder
import io.circe.generic.semiauto._
import org.specs2.Specification
import org.specs2.matcher.ThrownExpectations
import tg.monix.time.EpochSeconds
import io.circe.parser.decode

class OAuthRequestSpec extends Specification with ThrownExpectations {
  def is = s2"""
  OAuthRequest should
    encode token request correctly $encode
"""

  def encode = {
    val keyPair    = KeyPairGenerator.getInstance("RSA").generateKeyPair()
    val privateKey = keyPair.getPrivate
    val publicKey  = keyPair.getPublic

    val request       = OAuthRequest.from("email", EpochSeconds(0), EpochSeconds(1))
    val signedRequest = request.signedRequest(privateKey)
    val parts         = signedRequest.split("\\.").toList

    parts must haveSize(3)
    unBase64(parts.head) must be_==(OAuthRequest.rawHeader)
    decode[OAuthRequest](unBase64(parts(1))) must beRight(request)
    verify(publicKey, request.request, parts(2)) must beTrue
  }

  private implicit val decoder: Decoder[OAuthRequest] = deriveDecoder[OAuthRequest]
  private def unBase64(s: String)                     = new String(Base64.getUrlDecoder.decode(s))
  private def verify(publicKey: PublicKey, request: String, signedRequest: String) = {
    val signature = Signature.getInstance("SHA256withRSA")
    signature.initVerify(publicKey)
    signature.update(request.getBytes("UTF-8"))
    signature.verify(Base64.getUrlDecoder.decode(signedRequest))
  }

}
