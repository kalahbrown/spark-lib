name := "spark-scala-lib"
version := "0.0.1"
scalaVersion := "2.10.6"

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-optimize",
  "-Yinline-warnings"
)

fork := true

javaOptions += "-Xmx2G"

parallelExecution in Test := false


libraryDependencies ++= Seq(
	("org.apache.spark" % "spark-core_2.10" % "1.4.1"  % "provided"),
	("org.apache.spark" % "spark-streaming_2.10" % "1.4.1" % "provided"),
	("org.apache.avro" % "avro" % "1.7.7"),
	("org.apache.avro" % "avro-mapred" % "1.7.7" % "provided" classifier("hadoop2") ),
	("org.apache.hadoop" % "hadoop-common" % "2.7.1"  % "provided").
		excludeAll(ExclusionRule(organization ="javax.servlet")).
		excludeAll(ExclusionRule(organization ="org.eclipse.jetty.orbit")).
		excludeAll(ExclusionRule(organization ="org.mortbay.jetty"))
)


