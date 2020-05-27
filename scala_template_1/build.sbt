name := "scala_automation"

version := "1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

//// https://mvnrepository.com/artifact/org.locationtech.jts/jts-core
//libraryDependencies += "org.locationtech.jts" % "jts-core" % "1.15.0"
//
//// https://mvnrepository.com/artifact/org.locationtech.geotrellis/geotrellis-proj4
//libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-proj4" % "2.2.0"


resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"