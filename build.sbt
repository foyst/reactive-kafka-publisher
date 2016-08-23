enablePlugins(DockerPlugin)

name := """reactive-kafka-publisher"""

version := "2.0"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.9"
val akkaStreamVersion = "2.0"
val kafkaVersion = "0.9.0.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.11-M3",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "io.spray" %%  "spray-json" % "1.3.2",

  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
)

val assemNoTest = assembly
test in assemNoTest := {}

docker <<= (docker dependsOn assembly)
addArtifact(artifact in(Compile, assembly), assemNoTest)

dockerfile in docker := {
  val artifact = (assemblyOutputPath in assembly).value
  val artifactTargetPath = s"/app/${artifact.name}"
  val startCommand = "java $APPLICATION_OPTS -jar " + artifactTargetPath

  new Dockerfile {
    from("java")
    add(artifact, artifactTargetPath)
    cmdRaw(startCommand)
  }
}

import sbtdocker.BuildOptions.Remove.Always
buildOptions in docker := BuildOptions(removeIntermediateContainers = Always)
imageNames in docker := Seq(
  ImageName(s"${name.value}:latest")
)