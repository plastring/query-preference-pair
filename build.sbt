name := "query-preference-pair"

version := "0.1"

scalaVersion := "2.10.7"

val sparkVersion = "1.6.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion
)