import sbt._
import sbt.Keys._

name := "scaliak"

organization := "io.megam"

scalaVersion := "2.11.6"

scalacOptions := Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-optimise",
  "-Xcheckinit",
  "-Xlint",
  "-Xverify",
  "-Yinline",
  "-Yclosure-elim",
  "-Yconst-opt",
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
    val scalazVersion = "7.1.2"
    Seq(
      "org.json" % "json" % "20141113",
      "org.scalaz" %% "scalaz-core" % scalazVersion,
      "org.scalaz" %% "scalaz-iteratee" % scalazVersion,
      "org.scalaz" %% "scalaz-effect" % scalazVersion,
      "org.scalaz" %% "scalaz-concurrent" % scalazVersion % "test",
      "net.liftweb" %% "lift-json-scalaz7" % "3.0-M5-1",
      "com.basho.riak" % "riak-client" % "2.0.1",
      "org.apache.commons" % "commons-pool2" % "2.3",
      "org.slf4j" % "slf4j-api" % "1.7.12",
      "org.specs2" %% "specs2-core" % "3.6" % "test",
      "org.specs2" %% "specs2-matcher-extra" % "3.6" % "test"

      )
    }


    resolvers += Resolver.bintrayRepo("scalaz", "releases")

    logBuffered := false

    lazy val commonSettings = Seq(
      version in ThisBuild := "0.12",
      organization in ThisBuild := "Megam Systems"
    )

    lazy val root = (project in file(".")).
    settings(commonSettings).
    settings(
    sbtPlugin := true,
    name := "scaliak",
    description := """This is the fork of scaliak https://github.com/stackmob/scaliak upgraded to scala 2.11 and scalaz 7.1.2. We primarily use it  in our API Gateway : https://github.com/megamsys/megam_gateway.git
    Feel free to collaborate at https://github.com/megamsys/scaliak.git.""",
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    publishMavenStyle := false,
    bintrayOrganization := Some("megamsys"),
    bintrayRepository := "scala"
  )
