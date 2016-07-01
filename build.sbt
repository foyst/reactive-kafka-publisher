name := """reactive-kafka-publisher"""

version := "2.0"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.6"
val akkaStreamVersion = "2.0"
val kafkaVersion = "0.9.0.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.11-M3",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
)