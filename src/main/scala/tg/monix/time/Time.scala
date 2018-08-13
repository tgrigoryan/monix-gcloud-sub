package tg.monix.time

import java.time.Instant

import monix.eval.Task

trait Time {
  val now: Task[EpochSeconds] = Task.eval(Instant.now().getEpochSecond).map(EpochSeconds.apply)
}

object Time extends Time

//TODO - ensure positive seconds
case class EpochSeconds(seconds: Long) extends AnyVal { self ⇒
  def >=(point: EpochSeconds): Boolean = seconds >= point.seconds
  def +(seconds: Long): EpochSeconds = EpochSeconds(self.seconds + seconds)
}
