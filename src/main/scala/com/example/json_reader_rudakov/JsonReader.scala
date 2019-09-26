package com.example.json_reader_rudakov

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

object JsonReader extends App {
  case class User(id: Option[Int], country: Option[String], points: Option[Int], price: Option[Int], title: Option[String], variety: Option[String], winery: Option[String])

  implicit val formats: AnyRef with Formats = {

    Serialization.formats(FullTypeHints(List(classOf[User])))

  }

  val spark: SparkSession = SparkSession.builder().appName("JsonReader").getOrCreate()
  val sc = spark.sparkContext
  val scFolder = args(0)


  val lines: RDD[String] = sc.textFile(s"$scFolder/winemag-data-130k-v2.json")
  lines.map(parse(_,true).extract[User]).foreach(x => println(x.toString))

}

