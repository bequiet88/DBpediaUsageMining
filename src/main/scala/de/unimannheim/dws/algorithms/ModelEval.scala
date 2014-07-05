package de.unimannheim.dws.algorithms

import scala.math._

case class ModelEval(
  val clusterInfo: String,
  val time: Long,
  val triples: List[((String, String, String), String)]) {

  def getTime = time

  def getNoiseRel = {
    val noiseElems = triples.filter(t => t._2.equals("noise"))
    BigDecimal(noiseElems.size.toFloat / triples.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
  }

  def getNoClusters = {
    triples.groupBy(_._2).filter(p => !p._1.equals("noise")).size.toString
  }

  def getAvgElemCluster = {
    val groupedTriples = triples.groupBy(_._2).filter(p => !p._1.equals("noise"))
    val countedClusters = groupedTriples.map(_._2.size.toDouble)
    val avg = countedClusters.reduceLeft(_ + _) / countedClusters.size
    BigDecimal(avg).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
  }

  def getElemClustSD = {

    val groupedTriples = triples.groupBy(_._2).filter(p => !p._1.equals("noise"))
    val countedClusters = groupedTriples.map(_._2.size.toDouble).toList
    //    val avg = try { Some(getElemCluster.toDouble) } catch { case _ => None }
    val avg = countedClusters.reduceLeft(_ + _) / countedClusters.size
    BigDecimal(stdDev(countedClusters, avg)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString
  }

  def stdDev(list: List[Double], average: Double) = list.isEmpty match {
    case false =>
      val squared = list.foldLeft(0.0)(_ + squaredDifference(_, average))
      sqrt(squared / list.length.toDouble)
    case true => 0.0
  }

  def squaredDifference(value1: Double, value2: Double) = pow(value1 - value2, 2.0)

}

