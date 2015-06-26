name := "scaliak"

organization := "io.megam"

scalaVersion := "2.11.7"

description := """This is the fork of scaliak https://github.com/stackmob/scaliak upgraded to scala 2.11 and scalaz 7.1.2. We primarily use it  in our API Gateway : https://github.com/megamsys/megam_gateway.git
Feel free to collaborate at https://github.com/megamsys/scaliak.git."""

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

bintrayOrganization := Some("megamsys")

bintrayRepository := "scala"

publishMavenStyle := true

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
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:reflectiveCalls",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-Ydead-code")

  incOptions := incOptions.value.withNameHashing(true)

  resolvers ++= Seq(Resolver.sonatypeRepo("releases"), Resolver.sonatypeRepo("snapshots"),
  Resolver.bintrayRepo("scalaz", "releases")
)

  libraryDependencies ++= {
    val scalazVersion = "7.1.3"
    Seq(
      "org.json" % "json" % "20141113",
      "org.scalaz" %% "scalaz-core" % scalazVersion,
      "org.scalaz" %% "scalaz-iteratee" % scalazVersion,
      "org.scalaz" %% "scalaz-effect" % scalazVersion,
      "org.scalaz" %% "scalaz-concurrent" % scalazVersion % "test",
      "net.liftweb" %% "lift-json-scalaz7" % "3.0-M5-1",
      "com.basho.riak" % "riak-client" % "2.0.1",
      "org.apache.commons" % "commons-pool2" % "2.4.1",
      "org.slf4j" % "slf4j-api" % "1.7.12",
      "org.specs2" %% "specs2-core" % "3.6.1-20150618235732-d4f57e9" % "test",
      "org.specs2" %% "specs2-matcher-extra" % "3.6.1-20150618235732-d4f57e9" % "test"

      )
    }
