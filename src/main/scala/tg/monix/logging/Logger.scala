package tg.monix.logging

import monix.eval.Task

trait Logger {
  def log(message: String): Task[Unit]
}

object Logger {
  val console: Logger = (message: String) => Task.eval(println(message))
}
