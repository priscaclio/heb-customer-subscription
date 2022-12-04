package fr.episen.dataprocesing

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scopt.OParser
//import org.zalando.spark.jsonschema.SchemaConverter

import scala.io.Source

object Delete {

  //Scopt pour définir les arguments pour le service
  case class Config(
     delete: Boolean = false,
     hasher: Boolean = false,
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
      opt[Boolean]('d', "delete")
        //.required()
        .action((d, c) => c.copy(delete = d))
        .text("required boolean"),
      opt[Boolean]('h', "hasher")
        .action((h, c) => c.copy(hasher = h))
        .text("required boolean"),
      opt[Long]('u', "id")
        .action((u, c) => c.copy(id = u))
        .text("required long id "),
      opt[String]('i', "inputpath")
        .action((i, c) => c.copy(inputpath = i))
        .text("required string path with csv file"),
      opt[String]('o', "outputpath")
        .action((o, c) => c.copy(outputpath = o))
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


  def main(args: Array[String]): Unit = {

    OParser.parse(argParser, args, Config()) match {
      case Some(config) =>
        println(OParser.usage(argParser))
      // do stuff with config
        println("res delete args "+config.delete)
        println("res delete args "+config.hasher)
        println("res delete args "+config.id)
        println("res delete args "+config.inputpath)
        println("res delete args "+config.outputpath)
      case _ =>
        //println("you have to put some args")
        //println(OParser.usage(argParser))

    }

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
  }
}
