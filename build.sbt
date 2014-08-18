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

scalaVersion := "2.11.2"

scalacOptions := Seq(
  "-target:jvm-1.7",
  "-deprecation",
  "-feature",
  "-optimise",
  "-Xcheckinit",
  "-Xlint",
  "-Xverify",
  "-Yconst-opt",
  "-Yinline",
  "-Yclosure-elim",
  "-Ybackend:GenBCode",
  "closurify:delegating",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:reflectiveCalls",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-Ydead-code")

  incOptions := incOptions.value.withNameHashing(true)

  resolvers += "Twitter Repo" at "http://maven.twttr.com"

  resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases"

  resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/public"

  resolvers += "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots"

  resolvers  +=  "Sonatype Snapshots"  at  "https://oss.sonatype.org/content/repositories/snapshots"

  resolvers  += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

  resolvers += "JBoss" at "https://repository.jboss.org/nexus/content/groups/public"

  libraryDependencies ++= {
    val scalazVersion = "7.1.0"
    Seq(
      "org.json" % "json" % "20140107",
      "org.scalaz" %% "scalaz-core" % scalazVersion,
      "org.scalaz" %% "scalaz-iteratee" % scalazVersion,
      "org.scalaz" %% "scalaz-effect" % scalazVersion,
      "org.scalaz" %% "scalaz-concurrent" % scalazVersion % "test",
      "net.liftweb" %% "lift-json-scalaz7" % "2.6-RC1",
      "com.basho.riak" % "riak-client" % "2.0.0.RC1",
      "org.apache.commons" % "commons-pool2" % "2.2",
      "org.slf4j" % "slf4j-api" % "1.7.7",
      //"ch.qos.logback" % "logback-classic" % "1.1.2",
      "org.specs2" % "specs2_2.11" % "2.4.1" % "test"
      )
    }

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
          <url>https://github.com/megamsys/scaliak</url>
          <licenses>
          <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          </license>
          </licenses>
          <scm>
          <url>git@github.com:megamsys/scaliak.git</url>
          <connection>scm:git:git@github.com:megamsys/scaliak.git</connection>
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
