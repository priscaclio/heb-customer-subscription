package fr.episen.dataprocesing
package models

import org.joda.time.DateTime

case class Client(identifiantClient : Integer, nom : String,
                  prenom : String, adresse : String,
                  dateDeSouscription : DateTime)
