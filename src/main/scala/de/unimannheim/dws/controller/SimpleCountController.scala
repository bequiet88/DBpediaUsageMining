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

object SimpleCountController extends App {
  DbConn.openConn withSession { implicit session =>

    val testFiles = List("baw�", "einstein", "germany", "hockenheim", "matrix")

    testFiles.foreach(f => {
      val file: File = new File("D:/ownCloud/Data/Studium/Master_Thesis/04_Data_Results/testdata/" + f + "_test_triples_with_object.txt")
      readClassPropertyPairs(file)

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

  def readClassPropertyPairs(file: File)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

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

    val optionsList: List[Array[String]] = List(Array[String]("-O","-S", "interval", "-N", "3", "-R", "7"),
      Array[String]("-O","-S", "interval", "-N", "3"),
      Array[String]("-O","-S", "frequency", "-N", "3", "-R", "7"),
      Array[String]("-O","-S", "frequency", "-N", "3"))

    optionsList.foreach(o => {

      val options = o
      val countRes = SimpleCounter.retrieve(listTriples, options)
      countRes.map(r => println(r))

      val resList = SimpleCounter.getRankedTriples(listTriples, countRes)

      SimpleCounter.printResults(resList, options)

      resList.map(r => println(r))

    })

  }
}