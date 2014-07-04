package de.unimannheim.dws.controller

import java.io.File

import scala.io.Source
import scala.util.matching.Regex

import com.mongodb.casbah.commons.conversions.scala.RegisterConversionHelpers
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers

import de.unimannheim.dws.models.mongo.CommonLogFile
import de.unimannheim.dws.models.mongo.CommonLogFileDAO
import de.unimannheim.dws.preprocessing.LogFileParser

// http://www.tutorialspoint.com/scala/scala_regular_expressions.htm
// http://www.regexplanet.com/advanced/java/index.html

object LogFileController extends App {

  /*
   * Converters for Joda Time
   */
  RegisterConversionHelpers()
  RegisterJodaTimeConversionHelpers()

  /*
   * Set path to the log files
   */
  val filesHere = (new java.io.File("G:/usewod2014-dataset/DBpedia/all/")).listFiles //(new java.io.File("G:/usewod2014-dataset/DBpedia/dbp39/test/")).listFiles

  /*
   * Invoke method to read log files into mongoDB
   */
  createLogFiles(filesHere)

  /**
   * Method to create log file object on mongoDB
   */
  def createLogFiles(filesHere: Array[java.io.File]) = {
    //val path = "D:/Download/Uni_Projekte/Master_Thesis/usewod2014-dataset/USEWOD2014/data/DBpedia/dbpedia3.9/http19122013.log/http19122013.log"

    //val source = LogFileParser.readFile(path)

    for (source <- filesHere) {

      /*
   * Structure of Log Case Class as Regex
   */
      val format = List(new Regex("([a-z0-9.]+) "), // remote_host, dotted quad or resolved
        new Regex("([a-z0-9_-]+) "), // rfc921 usually -
        new Regex("([a-z0-9_-]+) "), // username if identified, else -
        new Regex("\\[(\\d{1,2}/\\w{3}/\\d{4})"), // date part
        new Regex("(\\d{1,2}:\\d{1,2}:\\d{1,2}) "), // time part
        new Regex("(\\+\\d{4})\\] "), // timezone part
        new Regex("\"([A-Z]{3,5}) "), // request method
        new Regex("(/[^ ]+) "), // request uri
        new Regex("([A-Z]+/\\d\\.\\d)\" "), // request protocol
        new Regex("(\\d+) ")) // response status code

      val logs = (for {
        line <- Source.fromFile(source).getLines().toList        
      } yield {
        val o = LogFileParser.parse(format, line)
        o
      })
      


      val cleanLogs = logs.filter(q => q != null)
      
      val sizeClean = logs.size
      
      
      /*
       * Dump to mongoDB
       */
      CommonLogFileDAO.insert(cleanLogs)

      //  val o_* = CommonLogFileDAO.find(ref = MongoDBObject("ip" -> "00cf8964d7c730b8e6e4cf2efe5d0d11"))
      //        .sort(orderBy = MongoDBObject("_id" -> -1)) // sort by _id desc
      //        .skip(1)
      //        .limit(1)
      //        .toList
      //        
      //  o_*.map(o => println(o.toString))      
    }
  }
}