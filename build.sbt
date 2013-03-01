import net.virtualvoid.sbt.graph.Plugin
import org.scalastyle.sbt.ScalastylePlugin
import ScaliakReleaseSteps._
import sbtrelease._
import ReleaseStateTransformations._
import ReleasePlugin._
import ReleaseKeys._
import sbt._

name := "scaliak"

organization := "com.stackmob"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1", "2.9.2", "2.10.0")

scalacOptions <++= (scalaVersion).map { version: String =>
  val defaults = Seq("-unchecked", "-deprecation")
  if (version.startsWith("2.10")) {
    defaults ++ Seq("-feature", "-language:implicitConversions,", "-language:higherKinds", "-language:reflectiveCalls")
  } else {
    defaults
  }
}

libraryDependencies ++= Seq(		          
  "org.scalaz" %% "scalaz-core" % "6.0.4",
  "net.liftweb" %% "lift-json-scalaz" % "2.5-RC1",
  "com.basho.riak" % "riak-client" % "1.1.0",
  "commons-pool" % "commons-pool" % "1.6",
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.specs2" %% "specs2" % "1.12.3" % "test",
  "org.pegdown" % "pegdown" % "1.0.2" % "test",
  "org.mockito" % "mockito-all" % "1.9.0" % "test"
)

logBuffered := false

Plugin.graphSettings

ScalastylePlugin.Settings

releaseSettings

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  setReadmeReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

publishTo <<= version { v: String =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}

publishMavenStyle := true

publishArtifact in Test := false

testOptions in Test += Tests.Argument("html", "console")

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/stackmob/scaliak</url>
  <licenses>
    <license>	
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:stackmob/scaliak.git</url>
    <connection>scm:git:git@github.com:stackmob/scaliak.git</connection>
  </scm>
  <developers>
    <developer>
      <id>jrwest</id>
      <name>Jordan West</name>
      <url>http://www.stackmob.com</url>
    </developer>
    <developer>
      <id>aaronschlesinger</id>
      <name>Aaron Schlesinger</name>
      <url>http://www.stackmob.com</url>
    </developer>
    <developer>
      <id>taylorleese</id>
      <name>Taylor Leese</name>
      <url>http://www.stackmob.com</url>
    </developer>
    <developer>
      <id>milesoconnell</id>
      <name>Miles O'Connell</name>
      <url>http://www.stackmob.com</url>
    </developer>
    <developer>
      <id>dougrapp</id>
      <name>Doug Rapp</name>
      <url>http://www.stackmob.com</url>
    </developer>
    <developer>
      <id>alexyakushev</id>
      <name>Alex Yakushev</name>
      <url>http://www.stackmob.com</url>
    </developer>
    <developer>
      <id>willpalmeri</id>
      <name>Will Palmeri</name>
      <url>http://www.stackmob.com</url>
    </developer>
  </developers>
)
