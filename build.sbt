ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "0.1.1"
ThisBuild / organization := "com.iomete"

val sparkVersion = "3.2.1"

lazy val root = (project in file(".")).
  settings(
    name := "data-compaction-job",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
    ),

    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
  )

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}