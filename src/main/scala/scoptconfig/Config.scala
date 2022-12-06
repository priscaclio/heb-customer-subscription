package fr.episen.dataprocesing
package scoptconfig

import scopt.OParser

object Config {

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

}
