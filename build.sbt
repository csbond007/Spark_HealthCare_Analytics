name := "Historical iHealth Analytics Application"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.1"     

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.1"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.1"

libraryDependencies += "com.typesafe" % "config" % "1.2.0"

jarName in assembly :="HistoricalHealthAnalyticsApp.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

