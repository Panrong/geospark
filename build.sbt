name := "geospark-scala2.12-spark3.0.0"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.datasyslab" % "geospark" % "1.3.2-SNAPSHOT"