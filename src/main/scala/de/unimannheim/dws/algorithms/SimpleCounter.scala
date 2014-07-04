package de.unimannheim.dws.algorithms

import scala.collection.JavaConverters._
import scala.slick.driver.JdbcDriver.backend.Database
import scala.slick.driver.PostgresDriver.simple._
import scala.util.control._

import com.hp.hpl.jena.ontology.OntClass

import de.unimannheim.dws.models.postgre.Tables._
import de.unimannheim.dws.preprocessing.DBpediaOntologyAccess
import de.unimannheim.dws.preprocessing.Util

object SimpleCounter extends RankingAlgorithm[ClassPropertyCounterRow, List[(String, String, Double)]] {

  /**
   * Method to generate class property pairs with their number of hits
   * possible improvements:
   * 1. use index on query_id
   */
  def generate()(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    val loop = new Breaks
    val limit = 1000000
    var offset = 0
    var mapClassProp: Map[(String, String), Int] = Map()

    loop.breakable {
      do {

        val queryClassProp = (EntityAnalytics flatMap { e =>
          PropertyAnalytics filter (p => e.tripleId === p.tripleId) map { p =>
            (e.classId, p.propertyId)
          }
        }).sortBy(r => r._1).drop(offset).take(limit)

        //        val queryClassProp = (for {
        ////          (e, p) <- EntityAnalytics innerJoin PropertyAnalytics on (_.queryId === _.queryId)
        //          e <- EntityAnalytics
        //          p <- PropertyAnalytics if e.queryId === p.queryId
        //        } yield (e.classId, p.propertyId)).sortBy(r => r._1).drop(offset).take(limit)

        println(queryClassProp.selectStatement)

        val listClassProp = queryClassProp.list

        if (listClassProp.size < 0) {

          mapClassProp = listClassProp.foldLeft(mapClassProp)((i, row) => {
            if (i.contains((row._1.get, row._2.get))) {
              i + (((row._1.get, row._2.get), i(row._1.get, row._2.get) + 1))
            } else {
              if (i.size % 1000 == 0) {
                println("Size of Map: " + i.size)
              }
              i + (((row._1.get, row._2.get), 1))
            }
          })
        }

        if (listClassProp.size < limit) {
          loop.break
        }
        offset = offset + limit
        println("Processed lines from DB: " + offset)
      } while (true)
    }

    //    val mapClassProp = (for {
    //      e <- EntityAnalytics
    //      p <- PropertyAnalytics if e.queryId === p.queryId
    //    } yield (e.classId.get, p.propertyId.get))

    //    foldLeft(Map[(String, String), Int]())((i, row) => {
    //
    //      if(i.size % 1000 == 0) {
    //        println("Size of Map: " + i.size)
    //      }
    //      
    //      if (i.contains((row._1, row._2))) {
    //        i + (((row._1, row._2), i(row._1, row._2) + 1))
    //      } else {
    //        i + (((row._1, row._2), 1))
    //      }
    //
    //    })

    //      
    //      EntityAnalytics.list.map(ent => {
    //      println("Started " + ent.entityId)
    //      val properties = (for {
    //        p <- PropertyAnalytics if p.tripleId === ent.tripleId
    //      } yield p).list
    //
    //      if (properties.size > 0) {
    //        properties.map(p => {
    //          (ent.classId.get, p.propertyId.get)
    //        })
    //
    //      } else List()
    //    })
    //      
    //    println("Lines read from DB: "+listClassProp.size)  
    //
    //        val resMap = listClassProp.groupBy(l => l).map(t => (t._1, t._2.length))

    println("Class Property Pairs extracted from DB: " + mapClassProp.size)

    val resList = mapClassProp.map(res => {
      ClassPropertyCounterRow(classId = res._1._1, propertyId = res._1._2, count = Some(res._2))
    })

    resList.toList
  }
  /**
   * Method to retrieve a sorted map of properties
   */
  def retrieve(triples: List[(String, String, String)], options: Array[String], entity: String)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    if (triples.size > 0) {
      val entityId = Util.md5(entity)

      // Filter out Top-N multiple triples - default is 0
      val props = {
        if (options.contains("-R")) {
          val props = triples.map(_._2).removeDuplicates
          props.filterNot(p => Util.getPropertiesToRemove.contains(p))
          //        val indexR = options.indexOf("-R")
          //        if (indexR + 1 < options.length) {
          //          try {
          //            val no = Integer.parseInt(options(indexR + 1))
          //            val sortedPropList = triples.groupBy(_._2).map(t => (t._1, t._2.length))
          //              .toList.sortBy({ _._2 }).reverse.map(_._1)
          //            sortedPropList.slice(no, sortedPropList.length)
          //          } catch {
          //            case t: Exception => triples.map(_._2).removeDuplicates
          //          }
          //        } else triples.map(_._2).removeDuplicates
        } else triples.map(_._2).removeDuplicates
      }

      val propertyIds = props.map(p => {
        Util.md5(p.trim())
      })

      /*
       * Read class label for this entity from DB
       */
      val classLabel = (for {
        (c, e) <- ClassesUnique innerJoin EntitiesUnique on (_.id === _.classId) if (e.id === entityId)
      } yield (c.id, c.label)).first

      /*
       * Get all properties requested together with this entity's class
       */
      val propertiesAll = (for {
        c <- ClassesUnique
        p <- PropertiesUnique
        cp <- ClassPropertyCounter
        if cp.classId === c.id
        if cp.propertyId === p.id
      } yield (c.label, (p.id, p.prefix, p.property, cp.count))).list.groupBy(l => l._1.get)

      val properties = propertiesAll.get(classLabel._2.get).getOrElse(List()).map(_._2).filter(p => propertyIds.contains(p._1))

      //      println("For " + classLabel._2 + " properties found on DB " + properties.size)

      /*
       * check whether props from DB present in this triple list's predicates
       */
      val resMapPropIds = properties.foldLeft((Map[String, Double](), propertyIds))((i, prop) => {
        //        println("Found props: "+ i._1.size + ", remaining size "+ i._2.size)

        val propLabel = prop._2.get + prop._3.get
        if (i._2.contains(prop._1)) {
          (i._1.+((propLabel, prop._4.get)), i._2 diff List(prop._1))
        } else {
          (i._1, i._2)
        }
      })

      /*
       * Check whether class subclasses exists and contain properties
       */
      val ontClass = DBpediaOntologyAccess.getOntClass(classLabel._2.get)
      val subClasses = ontClass.listSubClasses().asScala.toList diff List(ontClass)
      val subResMapPropIds = {
        if (subClasses.size > 0) {
          resMapGenerator(propertiesAll, resMapPropIds._2, subClasses, resMapPropIds._1, 1D)
        } else {
          (Map[String, Double](), resMapPropIds._2)
        }
      }

      /*
       * Check whether class super classes exists and contain properties
       */

      val superClasses = ontClass.listSuperClasses().asScala.toList diff List(ontClass)
      val superResMapPropIds = {
        if (superClasses.size > 0) {
          resMapGenerator(propertiesAll, subResMapPropIds._2, superClasses, subResMapPropIds._1, 1D)
        } else {
          (Map[String, Double](), subResMapPropIds._2)
        }
      }

      println("Processed Class: " + classLabel._2 + ", remaining properties: " + superResMapPropIds._2.size + ", found properties: " + superResMapPropIds._1.size)

      /*
       * In case all elements are full, return a sorted list, otherwise go into the recursion
       */
      val result = {
        if (superResMapPropIds._2.size > propertyIds.size / 2D) {
          recursiveRetrieval(propertiesAll, superResMapPropIds._2, ontClass, superResMapPropIds._1, 0.5D).toList.sortBy({ _._2 }).reverse
        } else superResMapPropIds._1.toList.sortBy({ _._2 }).reverse
      }

      /*
       * Determine bin size - default is 3
       */
      val noOfBins = {
        if (options.contains("-N")) {
          val indexN = options.indexOf("-N")
          if (indexN + 1 < options.length) {
            try {
              Integer.parseInt(options(indexN + 1))
            } catch {
              case t: Exception => 3
            }
          } else 3
        } else 3
      }

      /*
       * Determine discretizing function - default is by Frequency
       */
      if (options.contains("-S")) {
        val indexS = options.indexOf("-S")
        if (indexS + 1 < options.length) {
          if (options(indexS + 1).equals("interval")) discretizeByInterval(noOfBins, result)
          else if (options(indexS + 1).equals("frequency")) discretizeByFreq(noOfBins, result)
          else discretizeByInterval(noOfBins, result)
        } else discretizeByFreq(noOfBins, result)
      } else discretizeByFreq(noOfBins, result)

    } else List()
  }

  private def recursiveRetrieval(propertiesAll: Map[String, List[(Option[String], (String, Option[String], Option[String], Option[Int]))]], propertyIds: List[String], ontClass: OntClass, resMap: Map[String, Double], weight: Double)(implicit session: slick.driver.PostgresDriver.backend.Session): Map[String, Double] = {

    val superClass = ontClass.listSuperClasses(true).asScala.toList(0)
    val subClasses = superClass.listSubClasses(true).asScala.toList diff List(ontClass)

    /*
     * Find all leaf classes on this level of the ontology
     */
    val leafClasses = subClasses.foldLeft(List[OntClass]())((i, ontClass) => {
      /*
       * Subclasses that are directly leaf classes
       */
      if (ontClass.listSubClasses().asScala.toList.size == 0) {
        i :+ ontClass
      } /*
       * In case subclasses are NOT directly leaf classes, resolve their leaf sub classes
       */ else {
        val currentSubClasses = ontClass.listSubClasses().asScala.toList
        i ++ currentSubClasses.foldLeft(List[OntClass]())((j, ontSubClass) => {
          if (ontSubClass.listSubClasses().asScala.toList.size == 0) {
            j :+ ontSubClass
          } else j
        })
      }
    })

    val resMapPropIds: (Map[String, Double], List[String]) = resMapGenerator(propertiesAll, propertyIds, leafClasses, resMap, weight)

    println("Processed Super Class: " + superClass.getURI + ", remaining properties: " + resMapPropIds._2.size + ", found properties: " + resMapPropIds._1.size)

    if (resMapPropIds._2.size > 0 && superClass.listSuperClasses().asScala.toList.size > 0) {
      recursiveRetrieval(propertiesAll, resMapPropIds._2, superClass, resMapPropIds._1, weight / 2)
    } else {
      // code read label from all other properties
      //      val remainingPropIds = resMapPropIds._2.map(prop => {
      //        val propLabel = (for {
      //          p <- PropertiesUnique if p.id === prop
      //        } yield (p.prefix, p.property)).list
      //
      //        if (propLabel.size > 0)
      //          (propLabel.head._1.get + propLabel.head._2.get, 0D)
      //        else ("property not in data", 0D)
      //      }).toMap

      resMapPropIds._1 //.++(remainingPropIds)
    }
  }

  private def discretizeByInterval(noOfBins: Int, data: List[(String, Double)]): List[(String, String, Double)] = {

    val counts = data.map(_._2)

    val max = data max Ordering[Double].on[(_, Double)](_._2)
    val min = data min Ordering[Double].on[(_, Double)](_._2)

    val binSize = (max._2 - min._2) / noOfBins

    val resList = {
      for (i <- 0 to noOfBins - 1) yield {

        val values = {
          if (i == 0) {
            data.filter(r => r._2 <= binSize)
          } else if (i == (noOfBins - 1)) {
            data.filter(r => r._2 > binSize * i && r._2 <= (binSize * (i + 1)) + 1)
          } else {
            data.filter(r => r._2 > binSize * i && r._2 <= binSize * (i + 1))
          }
        }

        values.map(r => (r._1, i.toString, r._2))
      }
    }.toList.flatten

    resList.sortBy(r => (r._2, r._3)).reverse
  }

  private def discretizeByFreq(noOfBins: Int, data: List[(String, Double)]): List[(String, String, Double)] = {

    val binSizeDouble = (data.length) / noOfBins.toDouble
    val binSize = Util.round(binSizeDouble, 0).toInt

    val resList = {
      for (i <- 0 to noOfBins - 1) yield {

        val values = {
          if (i == 0) {
            data.reverse.slice(i * binSize, (i + 1) * binSize)
          } else if (i == (noOfBins - 1)) {
            data.reverse.slice((i * binSize), ((i + 1) * binSize) + 1)
          } else {
            data.reverse.slice((i * binSize), ((i + 1) * binSize))
          }
        }

        values.map(r => (r._1, i.toString, r._2))
      }
    }.toList.flatten

    resList.sortBy(r => (r._2, r._3)).reverse
  }

  private def resMapGenerator(propertiesAll: Map[String, List[(Option[String], (String, Option[String], Option[String], Option[Int]))]], propertyIds: List[String], leafClasses: List[OntClass], resMap: Map[String, Double], weight: Double)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    leafClasses.removeDuplicates.foldLeft((resMap, propertyIds))((i, classLabel) => {

      if (i._2.size > 0) {
        /*
       * Get all properties requested together with this entity's class
       */
        val properties = propertiesAll.get(classLabel.getURI()).getOrElse(List()).map(_._2).filter(p => propertyIds.contains(p._1))

        //        println("For " + classLabel.getURI() + " properties found on DB " + properties.size)

        /*
       * check whether props from DB present in this triple list's predicates
       */
        properties.foldLeft((i._1, i._2))((j, prop) => {
          val propLabel = prop._2.get + prop._3.get
          if (!j._1.contains(propLabel)) {
            (j._1.+((propLabel, prop._4.get * weight)), j._2 diff List(prop._1))
          } else {
            (j._1, i._2)
          }
        })
      } else (i._1, i._2)
    })
  }

  /**
   * *******************************
   * Methods for Slow Implementation
   * *******************************
   */

  /**
   * Method to retrieve a sorted map of properties
   */
  def retrieveSlow(triples: List[(String, String, String)])(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    if (triples.size > 0) {
      val entityId = Util.md5(triples.head._1)
      val propertyIds = triples.map(t => {
        Util.md5(t._2.trim())
      }).removeDuplicates

      /*
       * Read class label for this entity from DB
       */
      val classLabel = (for {
        (c, e) <- ClassesUnique innerJoin EntitiesUnique on (_.id === _.classId) if (e.id === entityId)
      } yield (c.id, c.label)).first

      /*
       * Get all properties requested together with this entity's class
       */
      val properties = (for {
        p <- PropertiesUnique
        cp <- ClassPropertyCounter
        if cp.classId === classLabel._1
        if cp.propertyId === p.id
        if cp.propertyId inSetBind propertyIds
      } yield (p.id, p.prefix, p.property, cp.count)).list

      println("For " + classLabel._2 + " properties found on DB " + properties.size)

      /*
       * check whether props from DB present in this triple list's predicates
       */
      val resMapPropIds = properties.foldLeft((Map[String, Double](), propertyIds))((i, prop) => {
        //        println("Found props: "+ i._1.size + ", remaining size "+ i._2.size)

        val propLabel = prop._2.get + prop._3.get
        if (i._2.contains(prop._1)) {
          (i._1.+((propLabel, prop._4.get)), i._2 diff List(prop._1))
        } else {
          (i._1, i._2)
        }
      })

      /*
       * Check whether class subclasses exists and contain properties
       */
      val ontClass = DBpediaOntologyAccess.getOntClass(classLabel._2.get)
      val subClasses = ontClass.listSubClasses().asScala.toList diff List(ontClass)
      val subResMapPropIds = {
        if (subClasses.size > 0) {
          resMapGeneratorSlow(resMapPropIds._2, subClasses, resMapPropIds._1, 1D)
        } else {
          (Map[String, Double](), resMapPropIds._2)
        }
      }

      /*
       * Check whether class superclasses exists and contain properties
       */

      val superClasses = ontClass.listSuperClasses().asScala.toList diff List(ontClass)
      val superResMapPropIds = {
        if (subClasses.size > 0) {
          resMapGeneratorSlow(subResMapPropIds._2, superClasses, subResMapPropIds._1, 1D)
        } else {
          (Map[String, Double](), subResMapPropIds._2)
        }
      }

      //      val fullResMap = resMapPropIds._1 ++ SubResMapPropIds._1.toList

      println("Processed Class: " + classLabel._2 + ", remaining properties: " + superResMapPropIds._2.size + ", found properties: " + superResMapPropIds._1.size)

      /*
       * In case all elements are full, return a sorted list, otherwise go into the recursion
       */
      if (superResMapPropIds._2.size > propertyIds.size / 2D) {
        recursiveRetrievalSlow(superResMapPropIds._2, ontClass, superResMapPropIds._1, 0.5D).toList.sortBy({ _._2 }).reverse
      } else superResMapPropIds._1.toList.sortBy({ _._2 }).reverse
    } else List()
  }

  private def recursiveRetrievalSlow(propertyIds: List[String], ontClass: OntClass, resMap: Map[String, Double], weight: Double)(implicit session: slick.driver.PostgresDriver.backend.Session): Map[String, Double] = {

    val superClass = ontClass.listSuperClasses(true).asScala.toList(0)
    val subClasses = superClass.listSubClasses(true).asScala.toList diff List(ontClass)

    /*
     * Find all leaf classes on this level of the ontology
     */
    val leafClasses = subClasses.foldLeft(List[OntClass]())((i, ontClass) => {
      /*
       * Subclasses that are directly leaf classes
       */
      if (ontClass.listSubClasses().asScala.toList.size == 0) {
        i :+ ontClass
      } /*
       * In case subclasses are NOT directly leaf classes, resolve their leaf sub classes
       */ else {
        val currentSubClasses = ontClass.listSubClasses().asScala.toList
        i ++ currentSubClasses.foldLeft(List[OntClass]())((j, ontSubClass) => {
          if (ontSubClass.listSubClasses().asScala.toList.size == 0) {
            j :+ ontSubClass
          } else j
        })
      }
    })

    val resMapPropIds: (Map[String, Double], List[String]) = resMapGeneratorSlow(propertyIds, leafClasses, resMap, weight)

    //    val fullResMap = resMap ++ resMapPropIds._1.toList

    println("Processed Super Class: " + superClass.getURI + ", remaining properties: " + resMapPropIds._2.size + ", found properties: " + resMapPropIds._1.size)

    if (resMapPropIds._2.size > 0 && superClass.listSuperClasses().asScala.toList.size > 0) {
      recursiveRetrievalSlow(resMapPropIds._2, superClass, resMapPropIds._1, weight / 2)
    } else {
      //      val remainingPropIds = resMapPropIds._2.map(prop => {
      //        val propLabel = (for {
      //          p <- PropertiesUnique if p.id === prop
      //        } yield (p.prefix, p.property)).list
      //
      //        if (propLabel.size > 0)
      //          (propLabel.head._1.get + propLabel.head._2.get, 0D)
      //        else ("property not in data", 0D)
      //      }).toMap

      resMapPropIds._1 //.++(remainingPropIds)
    }
  }

  private def resMapGeneratorSlow(propertyIds: List[String], leafClasses: List[OntClass], resMap: Map[String, Double], weight: Double)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    leafClasses.removeDuplicates.foldLeft((resMap, propertyIds))((i, classLabel) => {

      if (i._2.size > 0) {
        /*
       * Get all properties requested together with this entity's class
       */
        val properties = (for {
          p <- PropertiesUnique
          cp <- ClassPropertyCounter
          if cp.classId === Util.md5(classLabel.getURI())
          if cp.propertyId === p.id
          if cp.propertyId inSetBind i._2
        } yield (p.id, p.prefix, p.property, cp.count)).list

        println("For " + classLabel.getURI() + " properties found on DB " + properties.size)

        /*
       * check whether props from DB present in this triple list's predicates
       */
        properties.foldLeft((i._1, i._2))((j, prop) => {
          val propLabel = prop._2.get + prop._3.get
          if (!j._1.contains(propLabel)) {
            (j._1.+((propLabel, prop._4.get * weight)), j._2 diff List(prop._1))
          } else {
            (j._1, i._2)
          }
        })
      } else (i._1, i._2)
    })
  }

}