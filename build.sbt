
name := "stypes-calcite"

version := "0.1"

scalaVersion := "2.11.8"

val calciteVersion = "1.23.0-SNAPSHOT"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.calcite" % "calcite-csv" % calciteVersion
  ,"org.apache.calcite" % "calcite-core" % calciteVersion
  ,"org.scalatest" %% "scalatest" % "3.0.4" % "test"
  ,"junit" % "junit" % "4.10" % "test"
  ,"ch.qos.logback" % "logback-classic" % "1.2.3"
  ,"mysql" % "mysql-connector-java" % "8.0.21"
)