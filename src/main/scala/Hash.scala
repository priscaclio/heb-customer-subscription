package fr.episen.dataprocesing

import com.google.common.hash.Hashing
import java.nio.charset.StandardCharsets
import models.Client
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Hash {

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
    println("SparkSession crée")

    //    val list_id_client = args.toList
    //    val id_client_string = list_id_client.head
    //    val id_client_int = id_client_string.toInt
    val id_client_int = 1

    import sparkSession.implicits._
    val dataset = sparkSession.read.parquet("data").as[Client]
    dataset.show()
    val d = dataset.filter("identifiantClient != " + id_client_int)
    dataset.filter("identifiantClient = " + id_client_int)
      .map(client => Client(client.identifiantClient,hashage(client.nom),
      hashage(client.prenom),hashage(client.adresse),client.dateDeSouscription))
    dataset.union(d)
  }

  def hashage( str: String): String ={
    Hashing.md5().hashString(str, StandardCharsets.UTF_8).toString
  }
  }
