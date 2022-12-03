name := "heb-customer-subscription"

version := "0.1"

scalaVersion := "2.11.12"

idePackagePrefix := Some("fr.episen.dataprocesing")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"