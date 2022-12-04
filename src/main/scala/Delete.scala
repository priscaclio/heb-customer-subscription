package fr.episen.dataprocesing

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//import org.zalando.spark.jsonschema.SchemaConverter

import scala.io.Source

object Delete {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //val fileContents = Source.fromFile("data/schemaconfig.json").getLines.mkString
    //val schema = SchemaConverter.convertContent(fileContents)

    //val schema = SchemaConverter.convert("schemaconfig.json")

    val dataframe: DataFrame = sparkSession.read.option("header",true).csv("C:\\Users\\Prisca\\Documents\\data")

    dataframe.show()
    dataframe.printSchema()

    //test 1
    //dataframe.createOrReplaceTempView("tab")
    //val res = sparkSession.sql("select * from tab where IdentifiantClient=4")
    //res.show()

    //test 2
    dataframe.filter(dataframe("IdentifiantClient")!==1).show(false)
    val df = dataframe.filter(dataframe("IdentifiantClient")!==1).toDF()
    df.write.csv("C:/Users/Prisca/Documents/data/test2");
    //dataframe.write.format("csv").save("/tmp/spark_output/datacsv")
    //dataframe.write.mode(SaveMode.Overwrite).csv("data/data1.csv")
    //dataframe.filter(dataframe("IdentifiantClient")!==4).write.mode(SaveMode.Overwrite).csv("data/data1.csv")
    //dataframe.filter(dataframe("IdentifiantClient")!==4).write.format("csv").save("/data/data2.csv")
    //dataframe.show()
  }
}
