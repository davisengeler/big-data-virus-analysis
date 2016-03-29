import AssemblyKeys._

assemblySettings

name := "vScannerAWS"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-osgi" % "1.9.31" withSources()

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.4.0"

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
