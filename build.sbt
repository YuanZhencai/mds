name := "mds2"

version := "1.0"

organization := "com.wcs"

scalaVersion := "2.10.3"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-optimize")

resolvers ++= Seq(
  "sprayrepo" at "http://repo.spray.io",
  "snapshots" at "http://repo.akka.io/snapshots",
  "releases"  at "http://repo.akka.io/releases",
  "anormcypher" at "http://repo.anormcypher.org/",
  "typesafe"  at "http://repo.typesafe.com/typesafe/releases/")
                  
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.2.3"
, "com.typesafe.akka" %% "akka-kernel" % "2.2.3"
, "com.typesafe.akka" %% "akka-slf4j" % "2.2.3"
, "com.typesafe.slick" %% "slick" % "1.0.1"
, "io.spray" % "spray-http" % "1.2-RC4"
, "io.spray" % "spray-can" % "1.2-RC4"
, "io.spray" % "spray-routing" % "1.2-RC4"
, "io.argonaut" %% "argonaut" % "6.0.1"
, "org.anormcypher" %% "anormcypher" % "0.4.4"
, "ch.qos.logback" % "logback-classic" % "1.0.13"
, "postgresql" % "postgresql" % "9.1-901-1.jdbc4")

unmanagedBase <<= baseDirectory { base => base / "lib" }

