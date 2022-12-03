package fr.episen.dataprocesing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}



object Delete {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    // le type DateTime posait problème à cause de l'absence de DateTimeType
        val schema = StructType(
          Seq(
            StructField("IdentifiantClient", IntegerType),
            StructField("Nom", StringType),
            StructField("Prénom", StringType),
            StructField("Adresse", StringType),
            StructField("Date de Souscription", TimestampType)
          )
        )
  }
}
