package fr.episen.dataprocesing

import models.Client
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}



object Delete {
  def main(args: Array[String]): Unit = {
    println("début on crée la sparkSession")

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
    println("SparkSession crée")
//    val list_id_client = args.toList
//    val id_client_string = list_id_client.head
//    val id_client_int = id_client_string.toInt
      val id_client_int = 1

    //pour la récupération des csv
    val dataFrame: DataFrame = sparkSession.read
      .schema(schema)
      .option("header", value = true)
      .csv("csv")
    dataFrame.show()
    dataFrame.printSchema()

    import sparkSession.implicits._
    val d = dataFrame.filter("identifiantClient < 10").as[Client]
    val dataset = sparkSession.read.parquet("data").as[Client]
    dataset.show()
    println("dataset d =")
    d.show()
    println("dataset puis ")
    dataset.union(d)
    dataset.filter("identifiantClient != " + id_client_int)
    dataset.show()


  }
}
