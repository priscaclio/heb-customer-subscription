name := "heb-customer-subscription"

version := "0.1"

scalaVersion := "2.11.11"

idePackagePrefix := Some("fr.episen.dataprocesing")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
//libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"

libraryDependencies += "org.zalando" %% "spark-json-schema" % "0.6.1"

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
  )
}