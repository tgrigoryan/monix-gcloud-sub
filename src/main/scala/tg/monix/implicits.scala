package tg.monix

import cats.Show

object implicits {
  implicit val showThrowable: Show[Throwable] = Show.fromToString[Throwable]
}
