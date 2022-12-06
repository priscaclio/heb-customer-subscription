#heb-customer-subscription

Ce projet permet de créer une partie d'un système de compliance "GDPR".

Dans ce projet il y a deux services :
- l'un pour supprimer les données d'un client lorsqu'il le demande en donnant sont id 
- l'autre pour hasher les données d'un client lorsqu'il le demmande avec son id

## Table des matières
1. Initilialisation
2. Le code\
2.1. Scopt\
2.2. SparkSession
2.2. Schema\
2.4. Vérificaion de l'id\
2.5. Le service Delete\
2.6. Le service Hasher\
2.7 Cycle de vie
3. HDFS



### 1. Initilialisation
Création d'un projet ```Scala``` avec ```sbt```.  
Version de sbt ```1.7.1```\
Version de Scala ```2.11.11``` \
Version de Java ```8 ```

Pour Windows, il faut avoir installé ```winutils``` pour que le projet fonctionne.


### 2. Le code
#### 2.1 Scopt
Pour que le projet fonctionne il faut passer des arguments. Par exemple :
```
-i data -o C:\Users\Prisca\Documents\data\test5 -u 2 -s delete
```

####Pour cela on a utiliser scopt.

Dans le fichier built.sbt on a ajouté une dépendance : 

```sbt
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"
```

Et dans notre scala class object ```Config``` on a écrit le code 
pour définir nos arguments. On en a définit 4 :

```scala
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
```
Description des arguments :
```
heb-customer-subscription 1.0
Usage: heb-customer-subscription [options]

  -s, --service <value>    required string delete or hasher
  -u, --id <value>         required long id 
  -i, --inputpath <value>  required string path with csv file
  -o, --outputpath <value> required string path to put the new csv file
```

Les différents arguments :
- service : 

Cet argument sert à préciser quel service on souhaite exécuter. Le service ```delete```
ou le service ```hasher```. Si autre chose est spécifié dans cet argument, le programme indiquera 
a l'utilisateur qu'il doit spécifier soit 'delete' soit 'haser' en paramètre. 
```
Error: wong argument, you have to put 'delete' or 'hasher' with arg --service

heb-customer-subscription 1.0
Usage: heb-customer-subscription [options]

  -s, --service <value>    required string delete or hasher
  -u, --id <value>         required long id 
  -i, --inputpath <value>  required string path with csv file
  -o, --outputpath <value> required string path to put the new csv file
```

Cet argument est obligatoire.

- id :

Cet argument sert à préciser l'id du client pour lequel on souhaite faire le service.
L'argument est obligatoire. Dans le code il y aura une vérification que l'id donné
existe dans le fichier csv.

- inputpath :

Cet argument sert à préciser le chemin où se trouve le fichier csv sur lequel on veut appliquer des modifications.
Si on spécifie un dossier ça va lire tout les fichiers csv présent dans le dossier et si on donne le chemin directement
d'un fichier csv ça va lire uniquement le fichier csv.
L'argument est obligatoire. 

- outputpath :

Cet argument sert à préciser le chemin où on va écrire le nouveau fichier csv
avec les modifications apportées par le fichier. Il faut donner en chemin un ***dossier*** et non un fichier csv !
L'argument est obligatoire.

####Si on ne met pas d'argument lors de l'exécution du programme on a le retour suivant :

```
"C:\Program Files\Java\jdk1.8.0_202\bin\java.exe" ...

Error: Missing option --service
Error: Missing option --id
Error: Missing option --inputpath
Error: Missing option --outputpath
heb-customer-subscription 1.0
Usage: heb-customer-subscription [options]

  -s, --service <value>    required string delete or hasher
  -u, --id <value>         required long id 
  -i, --inputpath <value>  required string path with csv file
  -o, --outputpath <value>
                           required string path to put the new csv file

Process finished with exit code 0

```

Dans la scala class object ```Service```, dans le ```main``` on récupère les arguments
avec la variable ```config```.

```scala
OParser.parse(argParser, args, Config()) match {
  case Some(config) => 
    //le code
}
```
#### 2.2 SparkSession
On crée une sparksession pour pouvoir lire les csv, en écrire et pour faire du traitement sur les données.

On ajoute des dépendances dans le sbt :
```sbt
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
```

Le code scala :
```scala
val sparkSession = SparkSession.builder().master("local").getOrCreate()
```

Lecture du csv :
```scala
val dataframe: DataFrame = sparkSession.read.option("header",true).csv(config.inputpath)
```

#### 2.3 Schema
Pour vérifier que les données sont correctement typé on avait commencé en écrivant 
le code suivant :

```scala
val schema = StructType(
          Seq(
            StructField("IdentifiantClient", IntegerType),
            StructField("Nom", StringType),
            StructField("Prenom", StringType),
            StructField("Adresse", StringType),
            StructField("DateDeSouscription", TimestampType)
          )
)

val dataFrame: DataFrame = sparkSession.read
  .schema(schema)
  .option("header", value = true)
  .csv("csv")
dataFrame.show()
dataFrame.printSchema()
```
Puis on a essayé d'écrire un fichier de configuration en json comme proposé dans l'ennoncé.\
En cherchant dans la documentation on a trouvé ```spark-json-schema```.
On a essayé de faire le schéma avec ça en écrivant le code suivant :

Dans le sbt on a rajouté la dépendance suivante :
```sbt
libraryDependencies += "org.zalando" %% "spark-json-schema" % "0.6.1"
```

Le fichier de configuration json :
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "testSchema",
  "title": "Schema for customer subscription csv file",
  "type": "object",
  "properties": {
    "IdentifiantClient": {
      "$id": "#root/items/IdentifiantClient",
      "title": "IdentifiantClient",
      "type": "integer",
      "examples": [
        5
      ],
      "default": 0
    },
    "Nom": {
      "$id": "#root/items/Nom",
      "title": "Nom",
      "type": "string",
      "default": "",
      "examples": [
        "tet"
      ]
    },
    "Prenom": {
      "$id": "#root/items/Prenom",
      "title": "Prenom",
      "type": "string",
      "default": "",
      "examples": [
        "Prisca"
      ]
    },
    "Adresse": {
      "$id": "#root/items/Adresse",
      "title": "Adresse",
      "type": "string",
      "default": "",
      "examples": [
        "test"
      ]
    },
    "DateDeSouscription": {
      "$id": "#root/items/DateDeSouscription",
      "title": "DateDeSouscription",
      "type": "string", "format": "date",
      "default": "",
      "examples": [
        "12/12/2022"
      ]
    }
  },
  "required": [
    "IdentifiantClient",
    "Nom",
    "Prenom",
    "Adresse",
    "DateDeSouscription"
  ]

}
```
Le code scala :
```scala
val fileContents = Source.fromFile("config/schema2.json").getLines.mkString
val schema = SchemaConverter.convertContent(fileContents)
val dataframe2: DataFrame = sparkSession.read.option("header",true).schema(schema).csv(config.inputpath)
dataframe2.show()
dataframe2.printSchema()
```

Exemple lors de l'excécution : fichier avec des élements mal typé
```
+-----------------+----+------+-------+------------------+
|IdentifiantClient| Nom|Prenom|Adresse|DateDeSouscription|
+-----------------+----+------+-------+------------------+
|                1|Clio|Prisca|   test|        12/12/2022|
|                2|Clio|   Dan|   test|        12/12/2022|
|             test|Clio|   Dan|   test|              test|
|                3|Clio|   Pet|   test|        12/12/2022|
+-----------------+----+------+-------+------------------+
```
Il va rendre null la ligne où les données ne sont pas coorectement typées.
```
+-----------------+----+------+-------+------------------+
|IdentifiantClient| Nom|Prenom|Adresse|DateDeSouscription|
+-----------------+----+------+-------+------------------+
|                1|Clio|Prisca|   test|        12/12/2022|
|                2|Clio|   Dan|   test|        12/12/2022|
|             null|null|  null|   null|              null|
|                3|Clio|   Pet|   test|        12/12/2022|
+-----------------+----+------+-------+------------------+

root
 |-- IdentifiantClient: long (nullable = true)
 |-- Nom: string (nullable = true)
 |-- Prenom: string (nullable = true)
 |-- Adresse: string (nullable = true)
 |-- DateDeSouscription: string (nullable = true)

```
Et dans le fichier de retour après le service la ligne qui n'est pas correctement typée 
n'est pas écrite :

```
+-----------------+--------------------------------+--------------------------------+--------------------------------+------------------+
|IdentifiantClient|Nom                             |Prenom                          |Adresse                         |DateDeSouscription|
+-----------------+--------------------------------+--------------------------------+--------------------------------+------------------+
|2                |edaae2d0dd3d807a40856298797e56be|97c8e6d0d14f4e242c3c37af68cc376c|098f6bcd4621d373cade4e832627b4f6|12/12/2022        |
|1                |Clio                            |Prisca                          |test                            |12/12/2022        |
|3                |Clio                            |Pet                             |test                            |12/12/2022        |
+-----------------+--------------------------------+--------------------------------+--------------------------------+------------------+
```

Et au dernier cours on a vu qu'on pouvait créer une case class ```JsonStruct``` et ```JsonObject```
et dans le main créer une variable ```dataframeSchema = StructType(fiels.map(f => { //écrire tout les cas}))```, avec field
notre fichier de configuration json de cette forme :

```json
{
  "columns": [
    {
      "name": "IdentifiantClient",
      "type": "LongType"
    },
    {
      "name": "Nom",
      "type": "StringType"
    },
    {
      "name": "Prenom",
      "type": "StringType"
    },
    {
      "name": "Adresse",
      "type": "StringType"
    },
    {
      "name": "DateDeSouscription",
      "type": "StringType"
    }

  ]
}
```

Mais on a pas eu le temps de l'implementer. 


#### 2.4 Vérificaion de l'id

On vérifie que l'id passé en argument existe et qu'il est unique si ce n'est pas le cas,
on écrit un message.

On compte le nombre de ligne dans le fichier csv où l'id passé en argument est présent :
```scala
val selectID = dataframe2.filter(col("IdentifiantClient")===config.id).count()
```
Si l'id n'est pas présent on renvoie un message.
```scala
if(selectID == 0){
  println("The specified ID does not exist")
}
```

Si l'id n'est pas unique on envoie un message qui présice le nombre de fois qu'il apparait et
on affiche les données.
```scala
if(selectID > 1){
  println("The provided ID is not unique, there are "+selectID+" lines")
  dataframe2.filter(col("IdentifiantClient")===config.id).show(false)
}
```

Si l'id est unique alors on exécute le service demandé (```delete``` ou ```hasher```).

#### 2.5 Le service Delete
Pour le service ```delete``` on va écrire un nouveau csv avec toute les données 
sauf la ligne où il y a l'id du client passé en argument.

```scala
if(config.service == "delete"){
    println("Service delete start")

    val df = dataframe2.filter(dataframe2("IdentifiantClient")!==config.id)
    df.show(false)
    df.write.option("header",true).mode(SaveMode.Overwrite).csv(config.outputpath)
}
```

Exemple : \
On a un fichier csv avec 4 lignes et on veut supprimer le client avec l'id 2. 
```
+-----------------+----+------+-------+------------------+
|IdentifiantClient| Nom|Prenom|Adresse|DateDeSouscription|
+-----------------+----+------+-------+------------------+
|                1|Clio|Prisca|   test|        12/12/2022|
|                2|Clio|  Dani|   test|        12/12/2022|
|                4|Clio|   Dan|   test|              test|
|                3|Clio|   Pet|   test|        12/12/2022|
+-----------------+----+------+-------+------------------+
```
Résultat :
```
+-----------------+----+------+-------+------------------+
|IdentifiantClient|Nom |Prenom|Adresse|DateDeSouscription|
+-----------------+----+------+-------+------------------+
|1                |Clio|Prisca|test   |12/12/2022        |
|4                |Clio|Dan   |test   |test              |
|3                |Clio|Pet   |test   |12/12/2022        |
+-----------------+----+------+-------+------------------+
```

#### 2.6 Le service Hasher
Pour le service ```hasher``` on va écrire un nouveau csv avec toute les données
et pour la ligne où il y a l'id du client passé en argument on va hasher le ```Nom```,
le ```Prénom``` et l'```Adresse```. \
Pour cela on a créé 2 datasets :
- l'un avec toute les données sauf celle du client auquel on doit hasher les données.
- et l'autre avec juste les données du client qu'on hash. 

Puis on fait l'union de ces deux datasets et on écrit le résultat dans un csv.

La fonction de hashage :
```scala
// hashage function
def hashage( str: String): String ={
  Hashing.md5().hashString(str, StandardCharsets.UTF_8).toString
}
```
Le service haser :
```scala
if(config.service == "hasher"){
    println("Service hasher start")
  
    import sparkSession.implicits._
    
    //dataset with data without the line with id = config.IdentifiantClient
    val ds1 = dataframe2.filter(dataframe2("IdentifiantClient")!==config.id)
      .map(client => (client.getLong(0),client.getString(1),
        client.getString(2),client.getString(3),client.getString(4)))
    
    //dataset with only the data with the line where id = config.IdentifiantClient and hashing data
    val ds2 = dataframe2.filter(dataframe2("IdentifiantClient")===config.id)
      .map(client => (client.getLong(0),hashage(client.getString(1)),
        hashage(client.getString(2)),hashage(client.getString(3)),client.getString(4)))
    
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
}
```

A noter :
la façon dont nous avons écrit ce service n'est pas très bonne. On avait 
reécrit le service en partant initialement d'un dataset et non d'un dataframe 
mais à l'excécution l'option ```.option("header",true)``` était ignoré
ce qui nous causait des erreurs, donc on a
gardé la version du code avec le dataframe.

Sinon avec le dataset on aurait écrit ceci :
```scala
if(config.service == "hasher"){
    println("Service hasher start")
  
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
}
```

Avec :
```scala
val dataset = sparkSession
.read
.option("header",true)
.option("delimiter",",")
.csv(config.inputpath)
.as[Customer]
```

Et la case class Customer :
```scala
case class Customer(IdentifiantClient: Int, Nom: String, Prenom :String, Adresse : String, DateDeSouscription : String)
```

Exemple : \
On a un fichier csv avec 4 lignes et on veut hasher les données du client avec l'id 2.
```
+-----------------+----+------+-------+------------------+
|IdentifiantClient| Nom|Prenom|Adresse|DateDeSouscription|
+-----------------+----+------+-------+------------------+
|                1|Clio|Prisca|   test|        12/12/2022|
|                2|Clio|  Dani|   test|        12/12/2022|
|                4|Clio|   Dan|   test|              test|
|                3|Clio|   Pet|   test|        12/12/2022|
+-----------------+----+------+-------+------------------+
```
Résultat :

```
+-----------------+--------------------------------+--------------------------------+--------------------------------+------------------+
|IdentifiantClient|Nom                             |Prenom                          |Adresse                         |DateDeSouscription|
+-----------------+--------------------------------+--------------------------------+--------------------------------+------------------+
|2                |edaae2d0dd3d807a40856298797e56be|97c8e6d0d14f4e242c3c37af68cc376c|098f6bcd4621d373cade4e832627b4f6|12/12/2022        |
|1                |Clio                            |Prisca                          |test                            |12/12/2022        |
|4                |Clio                            |Dan                             |test                            |test              |
|3                |Clio                            |Pet                             |test                            |12/12/2022        |
+-----------------+--------------------------------+--------------------------------+--------------------------------+------------------+
```
#### 2.7 Cycle de vie
Pour les anciennes données, on peut écrire un script qui va les supprimer et les remplacer par les nouvelles.
On va supprimer le fichier csv d'origine et le remplacer par celui créer par le projet.

### 3. HDFS
Nous n'avons pas encore réussi cette partie mais avec la documentation on a 
compris qu'il faudrait faire ceci : 

**Prérequis** : avoir un cluster Hadoop HDFS.

#### Mettre les données dans hdfs :
Se connecter en ssh au namenode. Et exécuter la commande suivante :
```
hdfs dfs -put /home/toto/monFichier.csv /toto/bronze/
```

- En local : 
il faut donner les chemins vers les fichiers csv dans hdfs.

Exemple :
```
-i hdfs://172.31.253.142:9000/toto/bronze/
-o hdfs://172.31.253.142:9000/toto/silver/
-u 2
-s delete
```
- Sur le cluster :

Il faut générer le jar du projet puis dans le namenode faire un spark-submit en présisant le master
et les arguments.

