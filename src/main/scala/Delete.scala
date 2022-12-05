package fr.episen.dataprocesing

import com.google.common.hash.Hashing
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scopt.OParser
import org.zalando.spark.jsonschema.SchemaConverter

import java.nio.charset.StandardCharsets
import scala.io.Source

object Delete {

  //Scopt pour définir les arguments pour le service
  case class Config(
     //delete: Boolean = false,
     //hasher: Boolean = false,
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
      /*opt[Boolean]('d', "delete")
        //.required()
        .action((d, c) => c.copy(delete = d))
        .text("required boolean"),
      opt[Boolean]('h', "hasher")
        .action((h, c) => c.copy(hasher = h))
        .text("required boolean"),*/
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
        .text("required string path to put the new csv file")//,
      /*checkConfig(c => {
        if (c.inputpath < c.outputpath) {
          success
        } else {
          failure("just cause")
        }
      })*/
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

        import org.apache.spark.sql.functions._

        //TODO try with file in hdfs
        val sparkSession = SparkSession.builder().master("local").getOrCreate()

        val dataframe: DataFrame = sparkSession.read.option("header",true).csv(config.inputpath)
        //print data
        dataframe.show()
        //print schema of csv file
        dataframe.printSchema()

        //TODO mapper data with json file config
        //Dataframe2
        println("dataframe2 test avec schema json file")
        val fileContents = Source.fromFile("config/schema2.json").getLines.mkString
        val schema = SchemaConverter.convertContent(fileContents)
        val dataframe2: DataFrame = sparkSession.read.option("header",true).schema(schema).csv(config.inputpath)
        //print data
        dataframe2.show()
        //print schema of csv file
        dataframe2.printSchema()

      //Vérif id customer exist and id customer is unique
        val selectID = dataframe2.filter(col("IdentifiantClient")===config.id).count()

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

            val df = dataframe2.filter(dataframe2("IdentifiantClient")!==config.id)
            df.show(false)
            //if the inputpath = outputpath that will delete data in inputpath and write can't be done
            df.write.option("header",true).mode(SaveMode.Overwrite).csv(config.outputpath)
            //TODO résoudre le pb de overwrite le csv de départ

          }
          //Service to hash customer data in a csv file
          if(config.service == "hasher"){
            println("Service hasher start")

            //TODO to improve
            import sparkSession.implicits._

            val d = dataframe2.filter(dataframe2("IdentifiantClient")!==config.id)
              .map(client => (client.getLong(0),client.getString(1),
                client.getString(2),client.getString(3),client.getString(4)))

            val d2 = dataframe2.filter(dataframe2("IdentifiantClient")===config.id)
              .map(client => (client.getLong(0),hashage(client.getString(1)),
                hashage(client.getString(2)),hashage(client.getString(3)),client.getString(4)))

            /*
            println("d2")
            d2.toDF("IdentifiantClient","Nom","Prenom","Adresse","DateDeSouscription").show(false)

            print("d")
            d.show(false)
            */

            val unionData = d2.union(d)
            println("joinedData")
            unionData.toDF("IdentifiantClient","Nom","Prenom","Adresse","DateDeSouscription").show(false)

            unionData.toDF("IdentifiantClient","Nom","Prenom","Adresse","DateDeSouscription").write.option("header",true).mode(SaveMode.Overwrite).csv(config.outputpath)
          }
        }


      case _ =>
        //println("you have to put some args")
        //println(OParser.usage(argParser))

    }
/*
    import org.apache.spark.sql.functions._

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //val fileContents = Source.fromFile("data/schemaconfig.json").getLines.mkString
    //val schema = SchemaConverter.convertContent(fileContents)

    //val schema = SchemaConverter.convert("schemaconfig.json")

    println(args.length)
    println(args(0) + " "+ args(1))

    //path -> C:\Users\Prisca\Documents\data
    val dataframe: DataFrame = sparkSession.read.option("header",true).csv(args(0))

    dataframe.show()
    dataframe.printSchema()

    //test 1
    //dataframe.createOrReplaceTempView("tab")
    //val res = sparkSession.sql("select * from tab where IdentifiantClient=4")
    //res.show()

    //test 2
    dataframe.filter(dataframe("IdentifiantClient")!==args(1)).show(false)
    val df = dataframe.filter(dataframe("IdentifiantClient")!==1).toDF()
    //df.write.option("header",true).csv("C:/Users/Prisca/Documents/data/test3");


    //dataframe.write.format("csv").save("/tmp/spark_output/datacsv")
    //dataframe.write.mode(SaveMode.Overwrite).csv("data/data1.csv")
    //dataframe.filter(dataframe("IdentifiantClient")!==4).write.mode(SaveMode.Overwrite).csv("data/data1.csv")
    //dataframe.filter(dataframe("IdentifiantClient")!==4).write.format("csv").save("/data/data2.csv")
    //dataframe.show()



  val structure = StructType(
              StructField("IdentifiantClient", LongType,
              StructField("Nom", LongType,
              StructField("Prenom", LongType,
              StructField("Adresse", LongType,
              StructField("DateDeSouscription", LongType
            )
 */
  }
}
