package de.unimannheim.dws.controller

import java.io.File
import scala.collection.JavaConverters._
import scala.io.Source
import scala.slick.driver.PostgresDriver.simple._
import de.unimannheim.dws.algorithms.ClusterGrouper
import de.unimannheim.dws.algorithms.RankingAlgorithm
import de.unimannheim.dws.model.ExchangeRDFTriple
import de.unimannheim.dws.models.postgre.DbConn
import de.unimannheim.dws.models.postgre.Tables._
import de.unimannheim.dws.preprocessing.Util
import de.unimannheim.dws.algorithms.Stopwatch
import de.unimannheim.dws.algorithms.ModelEval

object ClusterGrouperController extends App {
  DbConn.openConn withSession { implicit session =>

    val testFiles = List("matrix") //"bawü", "einstein", "germany", "hockenheim", "matrix")

    testFiles.foreach(f => {
      val file: File = new File("D:/ownCloud/Data/Studium/Master_Thesis/04_Data_Results/testdata/" + f + "_test_triples.txt")
      readObjectPropertyPairsClusterFile(file)

    })

  }

  def createPropertyPairs(stepSize: Int)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    val listPropPairs = ClusterGrouper.generate

    val upperLimit = (Util.round(listPropPairs.size / stepSize.asInstanceOf[Double], 0) + 1).asInstanceOf[Int]

    for {
      i <- (1 to upperLimit)

    } yield {

      val skip = i * stepSize

      val tempList = listPropPairs.slice(skip - stepSize, skip)

      /*
         * PostgreSQL insert
         */
      try {
        de.unimannheim.dws.models.postgre.Tables.PairCounter.insertAll(tempList: _*)
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
  
  def readObjectPropertyPairsClusterList(javaList: java.util.List[ExchangeRDFTriple], options: Array[String], entity: String): java.util.List[ExchangeRDFTriple] = {
    DbConn.openConn withSession { implicit session =>

      println("db conn successful");

      val scalaList = javaList.asScala.toList

      //      val entity = scalaList.head.getSub() //.split("/").last

      val subjTriples = scalaList.filter(_.getSub.equals(entity)).map(t => (t.getSub, t.getPred, t.getObj))
      val objTriples = scalaList.filterNot(_.getSub.equals(entity)).map(t => (t.getSub, t.getPred, t.getObj))

      val resList = clusterObjectPropertyPairs(subjTriples, objTriples, options, entity)

      println("after clustering");

      resList._1.map(t => new ExchangeRDFTriple(t._1._1, t._1._2, t._1._3, t._2)).asJava
    }
  }

  private def readObjectPropertyPairsClusterFile(file: File)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

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

    val entity = listTriples.head._1

    val subjTriples = listTriples.filter(_._1.equals(entity))
    val objTriples = listTriples.filterNot(_._1.equals(entity))

    val optionsList: List[Array[String]] = List(Array[String]("-C", "HierarchicalClusterer") /*Array[String]("-O", "-C", "CustomKMedoids", "-P", "-R", "7") ,
      Array[String]("-C", "CustomKMedoids","-P"),
      Array[String]("-C", "CustomKMedoids","-R","7"),
      Array[String]("-C", "CustomKMedoids"),
      Array[String]("-C", "DBSCAN","-P","-R","7"),
      Array[String]("-C", "DBSCAN","-P"),
      Array[String]("-C", "DBSCAN","-R","7"),
      Array[String]("-C", "DBSCAN"),
      Array[String]("-C", "HierarchicalClusterer","-P","-R","7"),
      Array[String]("-C", "HierarchicalClusterer","-P"),
      Array[String]("-C", "HierarchicalClusterer","-R","7"),
      Array[String]("-C", "HierarchicalClusterer")*/ )

    optionsList.foreach(o => {

      val sw = new Stopwatch
      sw.start
      val resList = clusterObjectPropertyPairs(subjTriples, objTriples, o, entity)
      sw.stop
      val time = sw.elapsedTime
      val modelEval = ModelEval(resList._2, time, resList._1)
      ClusterGrouper.printResults(o, entity, modelEval)

      resList._1.map(r => println(r))
    })
  }

  private def clusterObjectPropertyPairs(subjTriples: List[(String, String, String)], objTriples: List[(String, String, String)], options: Array[String], entity: String)(implicit session: slick.driver.PostgresDriver.backend.Session): (List[((String, String, String), String)], String) = {

    println("before clustering");

    try {

      val subjResList = {
        val clusterRes = ClusterGrouper.retrieve(subjTriples, options, entity)
        clusterRes._1.map(r => println(r))
        (ClusterGrouper.getRankedTriples(subjTriples, clusterRes._1), clusterRes._2)
      }

      if (objTriples.size > 0) {
        val objResList = {
          val clusterRes = ClusterGrouper.retrieve(objTriples, options, entity)
          val clusterResList = clusterRes._1.map(p => (p._1, "Object " + p._2, p._3))
          clusterRes._1.map(r => println(r))
          (ClusterGrouper.getRankedTriples(objTriples, clusterResList), clusterRes._2)
        }
        val resList = subjResList._1 ++ objResList._1
        val clusterInfo = subjResList._2 + objResList._2
        println("after clustering");
        (resList, clusterInfo)
      } else {
        println("after clustering");
        (subjResList._1, subjResList._2)
      }

    } catch {
      case e: Exception =>
        println("Clustering threw exception " + e.printStackTrace());
        (List[((String, String, String), String)](), "")
    }

  }

}