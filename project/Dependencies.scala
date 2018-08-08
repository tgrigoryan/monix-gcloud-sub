import sbt._

object Dependencies {
  lazy val monixVersion = "3.0.0-RC1"
  lazy val monix = Seq(
    "io.monix"  %% "monix" % monixVersion)

  lazy val catsVersion = "1.2.0"
  lazy val cats = Seq(
    "org.typelevel" %% "cats-core" % catsVersion)

  lazy val http4sVersion = "0.18.15"
  lazy val http4s = Seq(
    "org.http4s"      %% "http4s-blaze-server",
    "org.http4s"      %% "http4s-circe"
  ).map(_ % http4sVersion)

  lazy val circeVersion = "0.9.3"
  lazy val circe = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)

  lazy val specs2Version = "4.1.0"
  lazy val specs2 = Seq(
    "org.specs2" %% "specs2-junit",
    "org.specs2" %% "specs2-core",
    "org.specs2" %% "specs2-mock",
    "org.specs2" %% "specs2-scalacheck"
  ).map(_ % specs2Version)
}
