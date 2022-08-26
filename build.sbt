name := "Spark Demo"

version := "1.0"

scalaVersion := "2.12.15"

val sparkVersion = "3.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

assembly / assemblyJarName := "spark-demo_2.12-fatjar-1.0.jar"

// META-INF discarding
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
