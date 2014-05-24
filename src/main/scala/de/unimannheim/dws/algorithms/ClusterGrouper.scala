package de.unimannheim.dws.algorithms

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

import scala.collection.mutable.StringBuilder
import scala.slick.driver.PostgresDriver.simple.optionColumnExtensionMethods
import scala.slick.driver.PostgresDriver.simple.queryToAppliedQueryInvoker
import scala.slick.jdbc.{ StaticQuery => Q }

import de.unimannheim.dws.models.postgre.Tables.GetResultPairCounterRow
import de.unimannheim.dws.models.postgre.Tables.GetResultPropertiesUniqueRow
import de.unimannheim.dws.models.postgre.Tables.PairCounterRow
import de.unimannheim.dws.models.postgre.Tables.PropertiesUniqueRow
import de.unimannheim.dws.models.postgre.Tables.PropertyAnalytics
import de.unimannheim.dws.preprocessing.Util
import weka.clusterers.AbstractClusterer
import weka.clusterers.DBSCAN
import weka.clusterers.HierarchicalClusterer
import weka.core.Attribute
import weka.core.FastVector
import weka.core.Instance
import weka.core.Instances

object ClusterGrouper extends RankingAlgorithm[PairCounterRow, (List[(String, String, Double)], String)] {

  /**
   * Method to generate property pairs with their number of hits
   * possible improvements:
   * 1. use index on query_id
   */
  def generate()(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    /*
     * If duplicate properties on query level should apply, use List[String], otherwise Set[String]
     */
    val mapQueryProp = (for {
      p <- PropertyAnalytics
    } yield (p.queryId.get, p.propertyId.get)).foldLeft(Map[(Long), Set[String]]())((i, row) => {
      if (i.contains((row._1))) {
        i + (((row._1), i(row._1) + row._2))
      } else {
        i + (((row._1), Set(row._2)))
      }
    })

    /*
     * big set of all property pairs 
     * -> leaks memory if duplicate values are allowed
     */
    val setPropPairs = mapQueryProp.map(sp => {
      val pairs = for (x <- sp._2; y <- sp._2) yield (x, y)
      pairs
    }).toList.flatten

    println("Property Pairs extracted from DB: " + setPropPairs.size)

    /*
     * property pair (key), count (value) map
     */
    val mapPropPairCount = setPropPairs.foldLeft(Map[(String, String), Int]())((i, row) => {

      if (i.contains((row._1, row._2))) {
        i + (((row._1, row._2), i(row._1, row._2) + 1))
      } else {
        if (i.size % 1000 == 0) {
          println("Size of Map: " + i.size)
        }
        i + (((row._1, row._2), 1))
      }
    })

    println("Distinct Property Pairs extracted from DB: " + mapPropPairCount.size)

    val resList = mapPropPairCount.map(res => {
      PairCounterRow(prop1Id = res._1._1, prop2Id = res._1._2, count = Some(res._2))
    })

    resList.toList
  }

  /**
   * Method to retrieve a sorted map of properties
   * possible improvements
   * 1. use index
   */
  def retrieve(triples: List[(String, String, String)], options: Array[String], entity: String)(implicit session: slick.driver.PostgresDriver.backend.Session) = {

    // Use only Triples with URI object - if activated
    val triplesFiltered = {
      if (options.contains("-L")) {
        triples.filter(t => t._3.startsWith("http"))
      } else triples
    }

    // Determine the Cluster Algorithm to be used - default is Custom K-Medoids
    val clusterAlgo = {
      if (options.contains("-C")) {
        val indexC = options.indexOf("-C")
        if (indexC + 1 < options.length) {
          options(indexC + 1)
        } else "CustomKMedoids"
      } else "CustomKMedoids"
    }

    // Calculate the distance matrix of given triples and Integer-URL-Resolver
    val distanceMatrix = calculateDistanceMatrix(triplesFiltered, options)

    // Prints a Pajek compatible net
    printDistanceMatrixAsNet(triples.head._1, distanceMatrix._2, distanceMatrix._4)
    printDistanceMatrix(triples.head._1, distanceMatrix._1)

    // Pushes the Distance Matrix to the Interoperability Class
    DistanceMatrix.setDistanceMatrix(convertDistanceMatrixToWeka(distanceMatrix._1))

    /*
     * Instantiate WEKA, see 
     * - http://weka.wikispaces.com/Creating+an+ARFF+file
     * - http://weka.wikispaces.com/Use+Weka+in+your+Java+code#Clustering
     */
    // 1. set up attributes
    var atts: FastVector = new FastVector()
    // - string
    atts.addElement(new Attribute("att1"))
    // 2. create Instances object
    var data: Instances = new Instances("clusterGrouper", atts, 0);

    // 3. fill with data
    for {
      i <- 0 to distanceMatrix._2.size
    } yield {
      var vals: Array[Double] = new Array[Double](data.numAttributes())
      vals(0) = i
      data.add(new Instance(1.0, vals))
    }

    // 4. instantiate clusterer
    var clusterer = getClusterer(clusterAlgo)
    clusterer.buildClusterer(data) // build the clusterer
    println(clusterer.toString)

    println("# of clusters: " + clusterer.numberOfClusters())

    // 5. iterate with instances over cluster
    val resList = (for {
      i <- 0 to data.numInstances()
    } yield {
      var cluster = "noise"
      try {
        cluster = clusterer.clusterInstance(data.instance(i)).toString
      } catch {
        case t: Exception => // todo: handle error
      }
      val propLabel = distanceMatrix._2.get(i).getOrElse("")
      val support = {
        val filteredProp = distanceMatrix._3.filter(p => (p.prefix.get + p.property.get).equals(propLabel))
        filteredProp.headOption match {
          case p: Some[PropertiesUniqueRow] => p.get.support.get
          case _ => 0D
        }
      }
      (propLabel, cluster, support)
    }).toList

    // Sort Cluster-Property List by cluster median
    val clusterPropsMap = resList.filter(r => !r._1.equals("")).sortBy(r => r._3).reverse.groupBy(r => r._2)
    val sortedClusterMedianList = clusterPropsMap.map(c => {
      val median = {
        // length is odd, take element (n+1)/2 of list
        if (c._2.size.%(2) == 1) c._2(((c._2.size + 1) / 2) - 1)._3
        // length is even, take half of the both central objects
        else {
          val e1 = c._2((c._2.size / 2) - 1)._3
          val e2 = c._2((c._2.size / 2))._3
          (e1+e2)/2
        }
      }
      (c._1, median)
    }).toList.sortBy(_._2).reverse
    
    // Append the property lists of the clusters according their sorted order of cluster medians to a res list
    val res = sortedClusterMedianList.foldLeft(List[(String, String, Double)]())((i, row) => {
     i++clusterPropsMap(row._1)
    })
    

    (res, clusterer.toString())
  }

  private def calculateDistanceMatrix(triples: List[(String, String, String)], options: Array[String])(implicit session: slick.driver.PostgresDriver.backend.Session): (List[(Int, Int, Double)], Map[Int, String], List[PropertiesUniqueRow], List[(Int, Int, Double)]) = {

    // Filter out Top-N multiple triples - default is 0
    val properties = {
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

    println("Unique Properties generated from TripleList: " + properties.size)

    /* Filter out properties wo/ connections
     * 1. Build MD5->HTTP Map of all properties
     * 2. String Builder of that list
     * 3. Query to PairCounter
     * 4. Filter out all properties from List (1) that have not been used
     * 5. Generate Integer-Index->Uri Map and MD5->Integer-Index Map
     * 6. Generate sorted sequence
     * 7. Query for support
     * 8. Generate two collections
     * 9. Generate distance matrix
     * 10. Generate distance count matrix
     */
    val propertyIdMap = properties.foldLeft((Map[String, String]())) { (i, t) =>
      {
        val id = Util.md5(t)
        (i + ((id, t)))
      }
    }

    // string with all pairs for DB Select Statement
    val pairString = propertyIdMap.keySet.foldLeft(new StringBuilder())((i, row) => {
      if (i.length() == 0) i.append("'" + row + "'")
      else i.append(",'" + row + "'")
    })

    // List of Pairs found on DB with a count assigned to them
    val propPairWeightList = Q.queryNA[PairCounterRow]("select prop_1_id, prop_2_id, count from pair_counter where prop_1_id in (" + pairString.toString + ") and prop_2_id in (" + pairString.toString + ")").list
    println("Pairs read from DB: " + propPairWeightList.size)

    // Unique list of properties found on Database Table PairCounter
    val propsUsedSet = propPairWeightList.foldLeft(Set[String]())((i, row) => i ++ Set(row.prop1Id, row.prop2Id))
    val propertiesUsedList = propertyIdMap.filter(p => propsUsedSet.contains(p._1))
    //val filteredPropPairWeightList = propPairWeightList.filter(p => propsUsedSet.contains(p.prop1Id) || propsUsedSet.contains(p.prop2Id))

    println("Uniques Properties Used in Dataset: " + propertiesUsedList.size);

    /* Generate temporary Maps mapping 1.) Integer Index to URI 2.) MD5 to Integer Index
       * -P set means all unused properties are filtered out
       * -P unset means, use all properties
       */
    val propertyIdMaps = {
      if (options.contains("-P")) {
        propertiesUsedList.zipWithIndex.foldLeft((Map[Int, String](), Map[String, Int]())) { (i, row) =>
          {
            (i._1 + ((row._2, row._1._2)), i._2 + ((row._1._1, row._2)))
          }
        }
      } else {
        properties.zipWithIndex.foldLeft((Map[Int, String](), Map[String, Int]())) { (i, t) =>
          {
            val id = Util.md5(t._1)
            (i._1 + ((t._2, t._1)), i._2 + ((id, t._2)))
          }
        }
      }
    }

    // sorted sequence of integers
    val propertyIntIdsList = for (i <- 0 to propertyIdMaps._1.size - 1) yield i

    // List of Support of single properties from Triple List
    val propertyList = Q.queryNA[PropertiesUniqueRow]("select * from properties_unique where id in (" + pairString.toString + ")").list

    // Two collections: 1.) Map with two integer indexes as key and the count as value. 2.) List of unprocessed prop pairs 
    val propPairWeightUniqueLists = propPairWeightList.foldLeft((Map[(Int, Int), Double](), propPairWeightList))((i, row) => {

      // Ensures that the smaller ID is in first place as required by ELKI
      def getIds(a: ((Int, Int), Double)): (Int, Int) = {
        if (a._1._1 < a._1._2) (a._1._1, a._1._2)
        else (a._1._2, a._1._1)
      }

      // Checks whether there's another same element in the remaining prop pairs 
      val sameElem = (i._2 diff List(row)).filter(_.equalsByReverseIds(row)).headOption
      val rowTransformed = ((propertyIdMaps._2.get(row.prop1Id).get, propertyIdMaps._2.get(row.prop2Id).get), row.count.get.asInstanceOf[Double])
      val ids = getIds(rowTransformed)

      // Append the current element and either delete only the current elem or also the same elem 
      sameElem match {
        case x: Some[PairCounterRow] => {
          (i._1 + (((ids._1, ids._2), (row.count.get + x.get.count.get).asInstanceOf[Double])), i._2 diff List(row, sameElem.get))
        }
        case None => {
          if (i._1.contains(ids._1, ids._2) == true) {

            (i._1, i._2 diff List(row))

          } else {
            (i._1 + (((ids._1, ids._2), (row.count.get).asInstanceOf[Double])), i._2 diff List(row))
          }
        }
      }
    })

    println("Unique pairs read from DB: " + propPairWeightUniqueLists._1.size)
    //    propPairWeightUniqueLists._1.foreach(p => println(propertyIdMaps._1.get(p._1._1) + " " + propertyIdMaps._1.get(p._1._2) + " " + p._2))

    // Generate the Distance matrix of all prop pairs with a distance assigned to them
    val propertyDistanceMatrix = propertyIntIdsList.foldLeft(List[(Int, Int, Double)]())((i, id) => {

      // inner loop to iterate only over all pairs (ab/ba) once
      val listInnerLoop = propertyIntIdsList.slice(id, propertyIntIdsList.size)

      val tempPropMatrix = listInnerLoop.map(subId => {
        val count = propPairWeightUniqueLists._1.getOrElse((id, subId), 0D)
        val distance = 1 / (count + 5)
        (id, subId, distance)
        // Old calculation        
        //        if (count == 0D) {
        //          (id, subId, 10D)
        //        } else {
        //          (id, subId, 1 / count)
        //        }
      })
      i ++ tempPropMatrix
    })

    val propertyCountMatrix = propertyIntIdsList.foldLeft(List[(Int, Int, Double)]())((i, id) => {

      // inner loop to iterate only over all pairs (ab/ba) once
      val listInnerLoop = propertyIntIdsList.slice(id, propertyIntIdsList.size)

      val tempPropMatrix = listInnerLoop.map(subId => {
        val count = propPairWeightUniqueLists._1.getOrElse((id, subId), 0D)

        (id, subId, count)
      })
      i ++ tempPropMatrix
    })

    (propertyDistanceMatrix, propertyIdMaps._1, propertyList, propertyCountMatrix)

  }

  private def printDistanceMatrix(label: String, propertyMatrix: List[(Int, Int, Double)]) = {

    println("Pairs about to be written to file: " + propertyMatrix.size)
    // Print the Distance Matrix as required by ELKI 
    val printLabel = label.split("/").last
    val file: File = new File("D:/ownCloud/Data/Studium/Master_Thesis/04_Data_Results/distance_matrices/distance_matrix_" + printLabel + ".txt");
    file.getParentFile().mkdirs();

    val out: BufferedWriter = new BufferedWriter(new FileWriter(file));

    val last = propertyMatrix.reverse.head

    propertyMatrix.map(p => {
      out.write(p._1 + " " + p._2 + " " + p._3)
      if (!p.eq(last)) out.newLine()
    })
    out.flush()
    out.close()
  }

  private def printDistanceMatrixAsNet(label: String, mapIdProp: Map[Int, String], propertyMatrix: List[(Int, Int, Double)]) = {

    //    println("Pairs about to be written to file: " + propertyMatrix.size)
    // Print the Distance Matrix as required by Pajek
    val printLabel = label.split("/").last
    val file: File = new File("D:/ownCloud/Data/Studium/Master_Thesis/04_Data_Results/distance_matrices/distance_matrix_" + printLabel + ".net");
    file.getParentFile().mkdirs();

    val out: BufferedWriter = new BufferedWriter(new FileWriter(file));

    // Write Vertices
    out.write("*Vertices " + mapIdProp.size)
    out.newLine()
    val listIdProp = mapIdProp.toList.sortBy(_._1)
    listIdProp.foreach((k) => {
      out.write(k._1 + 1 + " \"" + k._2 + "\"")
      out.newLine()
    })
    // Write Edges
    out.write("*Edges :2 \"QueriedTogether\"")
    out.newLine()
    val lastEdge = propertyMatrix.reverse.head
    propertyMatrix.map(p => {
      out.write((p._1 + 1) + " " + (p._2 + 1) + " " + p._3.toInt)
      if (!p.eq(lastEdge)) out.newLine()
    })
    out.flush()
    out.close()
  }

  private def convertDistanceMatrixToWeka(distancesList: List[(Int, Int, Double)]): Map[(Integer, Integer), java.math.BigDecimal] = {

    distancesList.foldLeft(Map[(Integer, Integer), java.math.BigDecimal]())((i, row) => {

      i + (((row._1, row._2), new java.math.BigDecimal(row._3)))

      //      if(i.containsKey(row._1)) {
      //        i.get(row._1).put(row._2, new java.math.BigDecimal(row._3))
      //        i        
      //      }
      //      else {
      //        val newMap: HashMap[Integer, java.math.BigDecimal] = new HashMap()
      //        newMap.put(row._2, new java.math.BigDecimal(row._3))
      //        i.put(row._1, newMap)
      //        i
      //      }
    })

  }

  private def getClusterer(clusterAlgo: String): AbstractClusterer = {

    clusterAlgo match {
      case "DBSCAN" => {
        var options: Array[String] = new Array[String](6)
        options(0) = "-E"; // epsilon
        options(1) = "0.1"
        options(2) = "-D"; // distance function
        options(3) = "de.unimannheim.dws.algorithms.CustomDBSCANDataObject"
        options(4) = "-M"; // mintpts
        options(5) = "4"
        var clusterer: DBSCAN = new DBSCAN() // new instance of clusterer
        clusterer.setOptions(options) // set the options
        println(clusterAlgo);
        clusterer.getOptions().foreach(p => println(p))
        clusterer
      }
      case "CustomKMedian" => {
        var options: Array[String] = new Array[String](8)
        options(0) = "-N"; // number of clusters
        options(1) = "7"
        options(2) = "-A"; // distance function
        options(3) = "de.unimannheim.dws.algorithms.CustomPairWiseDistance"
        options(4) = "-I" // Maximum iterations
        options(5) = "100"
        //        options(6) = "-S" // Random number seed.
        //        options(7) = "908349"
        //        options(8) = "-output-debug-info"
        //        options(9) = "-init"
        //        options(10) = "1"
        options(6) = "-M"; // Replace missing values with mean/mode.
        options(7) = "-O" // Preserve order of instances.  
        var clusterer: CustomSimpleKMedian = new CustomSimpleKMedian() // new instance of clusterer
        clusterer.setOptions(options) // set the options
        println(clusterAlgo);
        clusterer.getOptions().foreach(p => println(p))
        clusterer
      }
      case "CustomKMedoids" => {
        var options: Array[String] = new Array[String](8)
        options(0) = "-N"; // number of clusters
        options(1) = "7"
        options(2) = "-A"; // distance function
        options(3) = "de.unimannheim.dws.algorithms.CustomPairWiseDistance"
        options(4) = "-I" // Maximum iterations
        options(5) = "100"
        //                options(6) = "-S" // Random number seed.
        //                options(7) = "908349"
        //        options(8) = "-output-debug-info"
        //        options(9) = "-init"
        //        options(10) = "1"
        options(6) = "-M"; // Replace missing values with mean/mode.
        options(7) = "-O" // Preserve order of instances.  
        var clusterer: CustomSimpleKMedoids = new CustomSimpleKMedoids() // new instance of clusterer
        clusterer.setOptions(options) // set the options
        println(clusterAlgo);
        clusterer.getOptions().foreach(p => println(p))
        clusterer
      }
      case "HierarchicalClusterer" => {
        var options: Array[String] = new Array[String](7)
        options(0) = "-N"; // # of clusters
        options(1) = "7"
        options(2) = "-A"; // distance function
        options(3) = "de.unimannheim.dws.algorithms.CustomPairWiseDistance"
        options(4) = "-L"; // Link type
        options(5) = "MEAN"
        options(6) = "-P"
        //                options(7) = "-B" // If set, distance is interpreted as branch length, otherwise it is node height.  
        var clusterer: HierarchicalClusterer = new HierarchicalClusterer() // new instance of clusterer
        clusterer.setOptions(options) // set the options
        println(clusterAlgo);
        clusterer.getOptions().foreach(p => println(p))
        clusterer
      }
    }
  }

}