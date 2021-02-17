name := "geospark-scala2.12-spark3.0.0"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"


resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.datasyslab" % "geospark" % "1.3.2-SNAPSHOT"

// Assembly settings
// fix "deduplicate: different file contents found" error
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
// skip the test during assembly
test in assembly := {}
// set an explicit main class
// mainClass in assembly := Some("com.example.Main")
