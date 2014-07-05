package de.unimannheim.dws.algorithms

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

abstract class RankingAlgorithm[T, S] {

  /**
   * Generate the aggregates
   */
  def generate()(implicit session: slick.driver.PostgresDriver.backend.Session): List[T]

  /**
   * Retrieve data from ranking algorithm
   */
  def retrieve(triples: List[(String, String, String)], options: Array[String], entity: String)(implicit session: slick.driver.PostgresDriver.backend.Session): S

  /**
   * Sort the triples
   */
  def getRankedTriples(triples: List[(String, String, String)], list: List[(String, String, Double)]): List[((String, String, String), String)] = {

    // Group triples by their properties
    val groupedTriples = triples.sortBy(_._2).groupBy(_._2)

    /* Returns 
     * 1. a list of ranked triples according the ranking of their properties
     * 2. a map of remaining properties and their triples holding all props that does not occur in the ranked property list
     */
    val resultCols = list.foldLeft((List[((String, String, String), String)](), groupedTriples)) { (i, row) =>
      {
        val values = i._2.get(row._1).getOrElse(List()).map(t => (t, row._2))

        (i._1 ++ ((values)), i._2.filterNot(r => r._1 == row._1))
      }
    }

    val remainingTriples = resultCols._2.foldLeft(List[((String, String, String), String)]()) { (i, row) =>
      {
        val values = row._2.map(t => (t, "noise"))
        i ++ values
      }
    }.sortBy(_._1._2)

    resultCols._1 ++ remainingTriples
  }

  /**
   * Print the result to file
   */
  def printResults(options: Array[String], entity: String, modelEval: ModelEval): Any = {

    val label = entity.split("/").last

    val fileName = options.foldLeft(new StringBuilder())((i, row) => {
      if (i.length() == 0) {
        if (modelEval.clusterInfo.length() == 0) i.append(label + "_counter_" + row)
        else i.append(label + "_cluster_" + row)
      } else i.append("_" + row)
    })

    val file: File = new File("D:/ownCloud/Data/Studium/Master_Thesis/04_Data_Results/modelEval/" + label + "/" + fileName + ".txt");
    file.getParentFile().mkdirs();

    val out: BufferedWriter = new BufferedWriter(new FileWriter(file));

    // model eval
    out.write(modelEval.getNoiseRel + " " + modelEval.getTime + " " + modelEval.getNoClusters + " " + modelEval.getAvgElemCluster + " " + modelEval.getElemClustSD)
    out.newLine()

    // clusterInfo
    if (modelEval.clusterInfo.length() > 0) {
      out.write(modelEval.clusterInfo)
      out.newLine()
    }

    // Ranked triples
    modelEval.triples.foldLeft("")((i, row) => {
      if (row._2.equals(i)) {
        out.write(row._2 + " " + row._1._1 + " " + row._1._2 + " " + row._1._3)
        out.newLine()
        row._2
      } else {
        out.newLine()
        out.write("Group " + row._2)
        out.newLine()
        out.write(row._2 + " " + row._1._1 + " " + row._1._2 + " " + row._1._3)
        out.newLine()
        row._2
      }

    })

    out.flush()
    out.close()

  }

}