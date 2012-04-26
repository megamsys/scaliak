name := "scaliak"

organization := "com.stackmob"

version := "0.1.1-EXTENDED"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1")

resolvers ++= Seq("Typesafe Repository (releases)" at "http://repo.typesafe.com/typesafe/releases/",
                  "Scala Tools Repository (snapshots)" at "http://scala-tools.org/repo-snapshots",
                  "Scala Tools Repository (releases)"  at "http://scala-tools.org/repo-releases",
                  "repo.codahale.com" at "http://repo.codahale.com"
)

libraryDependencies ++= Seq(		          
    "org.scalaz" %% "scalaz-core" % "6.0.4" withSources(),
    "net.liftweb" %% "lift-json-scalaz" % "2.4" withSources(),
    "com.basho.riak" % "riak-client" % "1.0.5" withSources(),
    "org.specs2" %% "specs2" % "1.9" % "test" withSources(),
    "org.mockito" % "mockito-all" % "1.9.0" % "test" withSources(),
    "commons-pool" % "commons-pool" % "1.5.6" withSources(),
    "com.codahale" %% "jerkson" % "0.5.0"
)

logBuffered := false
