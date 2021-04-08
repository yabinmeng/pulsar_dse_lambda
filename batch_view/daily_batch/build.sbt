lazy val root = (project in file(".")).
  settings(
    name := "dailybatch",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("com.example.dailybatch")
  )

resolvers += "DataStax Repo" at "https://repo.datastax.com/public-repos/"
val dseVersion = "6.8.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",

  "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1" % "provided",

  "com.typesafe" % "config" % "1.4.0",
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}