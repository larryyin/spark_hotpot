name := "geospark_template"

version := "v1"

scalaVersion := "2.11.8"

unmanagedBase := baseDirectory.value / "lib"

unmanagedBase in Compile := baseDirectory.value / "lib"

//// https://mvnrepository.com/artifact/com.springml/spark-sftp
//libraryDependencies += "com.springml" %% "spark-sftp" % "1.1.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

// https://mvnrepository.com/artifact/org.datasyslab/geospark
libraryDependencies += "org.datasyslab" % "geospark" % "1.3.1"

// https://mvnrepository.com/artifact/org.datasyslab/geospark-viz
libraryDependencies += "org.datasyslab" % "geospark-viz_2.3" % "1.3.1"

// https://mvnrepository.com/artifact/org.datasyslab/geospark-viz
libraryDependencies += "org.datasyslab" % "geospark-sql_2.3" % "1.3.1"

//// https://mvnrepository.com/artifact/com.vividsolutions/jts
//libraryDependencies += "com.vividsolutions" % "jts" % "1.13"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"





