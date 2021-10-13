name := "jack-learning-spark-v2"

version := "1.0"
//version of Scala
scalaVersion := "2.12.10"
// spark library dependencies
// change this to 3.0.0 when released
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.3",
  "org.apache.spark" %% "spark-sql"  % "3.0.3"
)
