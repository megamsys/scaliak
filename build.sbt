name := "scaliak"

organization := "com.stackmob"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1")

resolvers ++= Seq("Typesafe Repository (releases)" at "http://repo.typesafe.com/typesafe/releases/",
                  "Scala Tools Repository (snapshots)" at "http://scala-tools.org/repo-snapshots",
                  "Scala Tools Repository (releases)"  at "http://scala-tools.org/repo-releases"
)

libraryDependencies ++= Seq(		          
  "org.scalaz" %% "scalaz-core" % "6.0.3",
  "net.liftweb" %% "lift-json-scalaz" % "2.4",
  "com.basho.riak" % "riak-client" % "1.0.5",
  "commons-pool" % "commons-pool" % "1.5.6",
  "org.specs2" %% "specs2" % "1.9" % "test",
  "org.mockito" % "mockito-all" % "1.9.0" % "test"
)

releaseSettings

logBuffered := false

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

pomIncludeRepository := { x => false }

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
