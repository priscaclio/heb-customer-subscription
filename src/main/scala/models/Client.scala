package fr.episen.dataprocesing
package models

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp


case class Client(identifiantClient : Integer, nom : String,
                  prenom : String, adresse : String,
                  dateDeSouscription : Timestamp)
