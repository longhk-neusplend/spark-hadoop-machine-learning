val scala3Version = "2.12.8"

lazy val root = project
  .in(file("."))
  .settings(
    name := "basic_scala_project",
    version := "1.0",

    scalaVersion := scala3Version,
    crossTarget := baseDirectory.value / "../../spark-cluster/apps/sbt-jar",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.8",
      "org.apache.spark" %% "spark-sql" % "2.4.8"
    )
  )
