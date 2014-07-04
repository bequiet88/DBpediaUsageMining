package de.unimannheim.dws.controller

import de.unimannheim.dws.models.postgre.DbConn
import de.unimannheim.dws.algorithms.SimpleCounter
import de.unimannheim.dws.preprocessing.Util
import de.unimannheim.dws.models.postgre.Tables._
import scala.slick.driver.PostgresDriver.simple._
import de.unimannheim.dws.models.mongo.ClassPropertyCounter
import de.unimannheim.dws.models.mongo.ClassPropertyCounterDAO
import scala.io.Source
import java.io.File
import de.unimannheim.dws.model.ExchangeRDFTriple
import scala.collection.JavaConverters._
import de.unimannheim.dws.algorithms.ModelEval
import de.unimannheim.dws.algorithms.Stopwatch

object SimpleCountController extends App {
  DbConn.openConn withSession { implicit session =>

    val testFiles = List("bawü") //, "einstein", "germany", "hockenheim", "matrix")

    testFiles.foreach(f => {
      val file: File = new File("D:/ownCloud/Data/Studium/Master_Thesis/04_Data_Results/testdata/" + f + "_test_triples_with_object.txt")
      readObjectClassPropertyPairsFile(file)

    })
    //    createClassPropertyPairs(1000)

  }

  def createClassPropertyPairs(stepSize: Int)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    val listClassProp = SimpleCounter.generate

    val upperLimit = (Util.round(listClassProp.size / stepSize.asInstanceOf[Double], 0) + 1).asInstanceOf[Int]

    for {
      i <- (1 to upperLimit)

    } yield {

      val skip = i * stepSize

      val tempList = listClassProp.slice(skip - stepSize, skip)

      /*
         * PostgreSQL insert
         */
      try {
        de.unimannheim.dws.models.postgre.Tables.ClassPropertyCounter.insertAll(tempList: _*)
        println("PostgreSQL successful " + tempList.size + ", total " + skip)
      } catch {
        case e: java.sql.BatchUpdateException => println(e.getNextException())
      }

      //      /*
      //         * mongoDB insert
      //         */
      //      val mongoList = tempList.map(c => {
      //        val classLabel = ClassesUnique.filter(_.id === c.classId)
      //        val propertyLabel = PropertiesUnique.filter(_.id === c.propertyId)
      //        de.unimannheim.dws.models.mongo.ClassPropertyCounter(classLabel = classLabel.first.label.get, propertyLabel = propertyLabel.first.prefix.get + propertyLabel.first.property.get, count = c.count.get)
      //      })
      //      ClassPropertyCounterDAO.insert(mongoList)
      //      println("Mongo successful " + tempList.size + ", total " + skip)

    }
  }

  def readObjectClassPropertyPairsList(javaList: java.util.List[ExchangeRDFTriple], options: Array[String], entity: String): java.util.List[ExchangeRDFTriple] = {
    DbConn.openConn withSession { implicit session =>
      val scalaList = javaList.asScala.toList

      //      val entity = scalaList.head.getSub() //.split("/").last

      val subjTriples = scalaList.filter(_.getSub.equals(entity)).map(t => (t.getSub, t.getPred, t.getObj))
      val objTriples = scalaList.filterNot(_.getSub.equals(entity)).map(t => (t.getSub, t.getPred, t.getObj))

      val resList = countObjectClassPropertyPairs(subjTriples, objTriples, options, entity)

      resList.map(t => new ExchangeRDFTriple(t._1._1, t._1._2, t._1._3, t._2)).asJava
    }
  }

  private def readObjectClassPropertyPairsFile(file: File)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    val listLines: List[String] = Source.fromFile(file, "UTF-8").getLines.toList

    val listTriples = listLines.map(l => {
      val split = l.split(" ").toList
      if (split.size == 3) {
        (split(0), split(1), split(2))
      } else if (split.size > 3) {
        val objectLiteral = split.slice(2, split.size).foldLeft(new StringBuilder())((i, row) => {
          i.append(row + " ")
        })
        (split(0), split(1), objectLiteral.toString)
      } else {
        ("", "", "")
      }
    })

    val entity = listTriples.head._1 //.split("/").last

    val subjTriples = listTriples.filter(_._1.equals(entity))
    val objTriples = listTriples.filterNot(_._1.equals(entity))

    val optionsList: List[Array[String]] = List(Array[String]("-O", "-S", "interval", "-N", "3", "-R", "7") /*,
      Array[String]("-O","-S", "interval", "-N", "3"),
      Array[String]("-O","-S", "frequency", "-N", "3", "-R", "7"),
      Array[String]("-O","-S", "frequency", "-N", "3")*/ )

    optionsList.foreach(o => {
      val sw = new Stopwatch
      sw.start
      val resList = countObjectClassPropertyPairs(subjTriples, objTriples, o, entity)
      val time = sw.elapsedTime
      val modelEval = ModelEval("", time, resList)
      SimpleCounter.printResults(o, entity, modelEval)
      resList.map(r => println(r))
    })

  }

  private def countObjectClassPropertyPairs(subjTriples: List[(String, String, String)], objTriples: List[(String, String, String)], options: Array[String], entity: String)(implicit session: slick.driver.PostgresDriver.backend.Session): List[((String, String, String), String)] = {
    val subjResList = {
      val counterRes = SimpleCounter.retrieve(subjTriples, options, entity)
      SimpleCounter.getRankedTriples(subjTriples, counterRes)
    }

    if (objTriples.size > 0) {
      val objResList = {
        val counterRes = SimpleCounter.retrieve(objTriples, options, entity)
        val counterResList = counterRes.map(p => (p._1, "Object " + p._2, p._3))
        SimpleCounter.getRankedTriples(objTriples, counterResList)
      }

      subjResList ++ objResList
    } else subjResList

  }

}