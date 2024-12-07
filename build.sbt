name := "SimpleApacheBeamScala"

version := "0.1"

scalaVersion := "2.13.15"

// Apache Beam dependencies
libraryDependencies ++= Seq(
  "org.apache.beam" % "beam-sdks-java-core" % "2.48.0",
  "org.apache.beam" % "beam-runners-direct-java" % "2.48.0",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.beam" % "beam-sdks-java-io-kafka" % "2.48.0", 
)
resolvers += "Confluent" at "https://packages.confluent.io/maven/"
