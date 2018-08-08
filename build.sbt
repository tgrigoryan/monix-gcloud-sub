import Dependencies._

lazy val commonSettings = Seq(
  organization    := "tg",
  scalaVersion    := "2.12.4",
  version         := "0.1.0-SNAPSHOT",
  name            := "monix-gcloud-sub"
)

scalacOptions     ++= Seq(
  "-unchecked",
  "-feature",
  "-deprecation:false",
  "-Xfatal-warnings",
  "-Xcheckinit",
  "-Xlint",
  "-Xlint:-nullary-unit",
  "-Ypartial-unification",
  "-Ywarn-unused-import",
  "-Ywarn-numeric-widen",
  "-Ywarn-dead-code",
  "-Yno-adapted-args",
  "-language:_",
  "-target:jvm-1.8",
  "-encoding", "UTF-8"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    scalafmtOnCompile   := true,
    libraryDependencies ++= specs2.map(_ % Test) ++ cats ++ http4s ++ circe ++ monix
  )

//Make console usable
scalacOptions in (Compile, console) ~= (_.filterNot(Set(
  "-Ywarn-unused-import",
  "-Xfatal-warnings"
)))

//No warts
wartremoverErrors in (Compile, compile) ++=
  Warts.allBut(
    Wart.Any, Wart.DefaultArguments, Wart.Enumeration, Wart.ExplicitImplicitTypes, Wart.FinalCaseClass, Wart.ImplicitConversion,
    Wart.ImplicitParameter, Wart.LeakingSealed, Wart.Nothing, Wart.Overloading, Wart.NonUnitStatements, Wart.Null, Wart.Option2Iterable,
    Wart.Product, Wart.PublicInference, Wart.Recursion, Wart.Serializable, Wart.Throw, Wart.ToString
  )


//Only Specs 2
testFrameworks in Test := Seq(TestFrameworks.Specs2)

//Test Coverage
coverageMinimum       := 75
coverageFailOnMinimum := true
