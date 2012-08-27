name := "scaliak"

organization := "com.stackmob"

version := "0.2"

scalaVersion := "2.9.2"

crossScalaVersions := Seq("2.9.2")

resolvers ++= Seq("Typesafe Repository (releases)" at "http://repo.typesafe.com/typesafe/releases/",
                  "gideondk-repo" at "https://raw.github.com/gideondk/gideondk-mvn-repo/master",
                  "Scala Tools Repository (snapshots)" at "http://scala-tools.org/repo-snapshots",
                  "Scala Tools Repository (releases)"  at "http://scala-tools.org/repo-releases",
                  "repo.codahale.com" at "http://repo.codahale.com",
                  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(		          
    "com.basho.riak" % "riak-client" % "1.0.5" withSources(),
    "org.scalaz" %% "scalaz-core" % "7.0.0-M3" withSources(),
    "org.scalaz" %% "scalaz-effect" % "7.0.0-M3" withSources(),
    "net.debasishg" % "sjsonapp_2.9.1" % "0.1-scalaz-seven",
    "org.specs2" %% "specs2" % "1.9" % "test" withSources(),
    "org.mockito" % "mockito-all" % "1.9.0" % "test" withSources(),
    "commons-pool" % "commons-pool" % "1.5.6" withSources()
)

logBuffered := false
