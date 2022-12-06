package fr.episen.dataprocesing

import com.google.common.hash.Hashing
import fr.episen.dataprocesing.models.Customer
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.zalando.spark.jsonschema.SchemaConverter
import scopt.OParser

import java.nio.charset.StandardCharsets
import scala.io.Source

object Service2 {

  //Scopt pour définir les arguments pour le service

  //case class Client(IdentifiantClient: Long, Nom: String, Prenom :String, Adresse : String, DateDeSouscription : String)

  case class Config(
                     service: String = "",
                     id: Long = -1,
                     inputpath: String = "",
                     outputpath: String = ""
                   )

  val builder = OParser.builder[Config]
  val argParser = {
    import builder._
    OParser.sequence(
      programName("heb-customer-subscription"),
      head("heb-customer-subscription", "1.0"),
      opt[String]('s', "service")
        .action((h, c) => c.copy(service = h))
        .required()
        .validate(h => {
          if (h == "delete" || h == "hasher") {
            success
          } else {
            failure("wong argument, you have to put 'delete' or 'hasher' with arg --service")
          }
        })
        .text("required string delete or hasher"),
      opt[Long]('u', "id")
        .action((u, c) => c.copy(id = u))
        .required()
        .text("required long id "),
      opt[String]('i', "inputpath")
        .action((i, c) => c.copy(inputpath = i))
        .required()
        .text("required string path with csv file"),
      opt[String]('o', "outputpath")
        .action((o, c) => c.copy(outputpath = o))
        .required()
        .text("required string path to put the new csv file")
    )
  }

  def hashage( str: String): String ={
    Hashing.md5().hashString(str, StandardCharsets.UTF_8).toString
  }

  def main(args: Array[String]): Unit = {

    OParser.parse(argParser, args, Config()) match {
      case Some(config) =>
        println(OParser.usage(argParser))
        // do stuff with config
        println("res service args "+config.service)
        println("res args "+config.id)
        println("res args "+config.inputpath)
        println("res args "+config.outputpath)

        //Sparksession

        //TODO try with file in hdfs
        val sparkSession = SparkSession.builder().master("local").getOrCreate()

        //path = http://172.31.253.145:9000/...

        import org.apache.spark.sql.functions._
        import sparkSession.implicits._

        val dataset = sparkSession
          .read
          .option("header",true)
          .option("delimiter",",")
          .csv(config.inputpath)
          .as[Customer]

        dataset.show(false)

        //Vérif id customer exist and id customer is unique
        val selectID = dataset.filter(col("IdentifiantClient")===config.id).count()

        //if ID customer doesn't exist
        if(selectID == 0){
          println("The specified ID does not exist")
        }

        //if ID customer isn't unique
        if(selectID > 1){
          println("The provided ID is not unique, there are "+selectID+" lines")
        }

        //if ID custumer is unique
        if(selectID==1){

          //Service to delete a customer in a csv file
          if(config.service == "delete"){
            println("Service delete start")


            val df = dataset.filter(_.IdentifiantClient!=config.id)
            df.show(false)
            //if the inputpath = outputpath that will delete data in inputpath and write can't be done
            df.write.option("header",true).mode(SaveMode.Overwrite).csv(config.outputpath)
            //TODO résoudre le pb de overwrite le csv de départ

          }

          //Service to hash customer data in a csv file
          if(config.service == "hasher"){
            println("Service hasher start")

            //TODO to improve

            val ds1 = dataset.filter(_.IdentifiantClient!=config.id)
            val ds2 = dataset.filter(_.IdentifiantClient==config.id)
              .map(client => (client.IdentifiantClient,hashage(client.Nom),
                hashage(client.Prenom),hashage(client.Adresse),client.DateDeSouscription)).as[Customer]

            // combine the 2 datasets
            val unionData = ds2.union(ds1)
            println("joinedData")
            unionData.toDF("IdentifiantClient","Nom","Prenom","Adresse","DateDeSouscription").show(false)

            //write csv file data hasher for the given ID
            unionData
              .toDF("IdentifiantClient","Nom","Prenom","Adresse","DateDeSouscription")
              .write.option("header",true)
              .mode(SaveMode.Overwrite)
              .csv(config.outputpath)

            //TODO voir coalesce(1)
          }
        }


      case _ =>
      //?

    }
  }

}
