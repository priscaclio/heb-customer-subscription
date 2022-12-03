package fr.episen.dataprocesing

import fr.episen.dataprocesing.models.Client
import org.apache.spark.sql.{DataFrame, SparkSession}
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
    val list_id_client = args.toList
    val id_client_string = list_id_client.head
    val id_client_int = id_client_string.toInt

//    //pour la récupération des csv
//    val dataFrame: DataFrame = sparkSession.read
//      .schema(schema)
//      .option("header", value = true)
//      .csv("data")
//    dataFrame.show()
//    dataFrame.printSchema()
//
//    val dataset_filtrer = dataFrame.where("identifiantClient ="+id_client_int)


    import sparkSession.implicits._
    val dataset = sparkSession.read.parquet("data").as[Client]
    dataset.filter("identifiantClient != " + id_client_int)



  }
}
