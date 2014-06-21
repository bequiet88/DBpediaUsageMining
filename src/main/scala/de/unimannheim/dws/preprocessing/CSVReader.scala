package de.unimannheim.dws.preprocessing

import scala.io.Source
import org.apache.commons.csv.CSVFormat
import scala.collection.JavaConverters._
import org.apache.commons.csv.CSVPrinter
import org.joda.time.format.DateTimeFormat
import java.io.BufferedWriter
import org.joda.time.DateTime
import java.io.FileWriter
import java.io.File

object CSVReader extends App {

  // generate CSVPrinter Object
  val dateTimeFormatter = DateTimeFormat.forPattern("ddMMyy_kkmmss");
  val path = "D:/ownCloud/Data/Studium/Master_Thesis/04_Data_Results/statistics/statistics_" + DateTime.now().toString(dateTimeFormatter) + ".csv"
  implicit val writer: BufferedWriter = new BufferedWriter(new FileWriter(
    new File(path)));
  implicit val csvPrinter: CSVPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
  csvPrinter.print("sep=,")
  csvPrinter.println()

  val input = new File("D:/ownCloud/Data/Studium/Master_Thesis/04_Data_Results/statistics/main_query_patterns2.csv")

  val reader = Source.fromFile(input).reader

  val parser = CSVFormat.DEFAULT.parse(reader);
  //				CSVFormat.EXCEL.parse(reader);

  val res = parser.iterator().asScala.foldLeft(List[(String, String, String)]())((i, row) => {
    val elems = row.iterator().asScala.toList
    if(i.size%10000 == 0) println(i.size);
    i :+ (elems(0), elems(1), elems(2))
  })

  val patternDistribution2 = res.groupBy(l => l).map(t => (t._1, t._2.length))
    .toList.sortBy({ _._2 }).map(f => ("" + f._1, "" + f._2, "" + BigDecimal(f._2.toFloat / res.size).setScale(2, BigDecimal.RoundingMode.HALF_UP))).reverse

  writeOutputToFile("Main triple pattern types", res.+:(("Pattern", "Abs. Number", "Rel. Number")).slice(0, 11))
  /*  
   *  Anonymous function to write data to the open csv file
   */
  def writeOutputToFile(title: String, data: List[(String, String, String)])(implicit writer: BufferedWriter, csvPrinter: CSVPrinter) = {
    csvPrinter.println
    csvPrinter.print(title)
    csvPrinter.println
    /*
	   * Row 1
	   */
    for (stat <- data) {
      csvPrinter.print(stat._1)
    }
    csvPrinter.println

    /*
	   * Row 2
	   */
    for (stat <- data) {
      csvPrinter.print(stat._2)
    }
    csvPrinter.println

    /*
	   * Row 3
	   */
    for (stat <- data) {
      csvPrinter.print(stat._3)
    }
    csvPrinter.println
    csvPrinter.flush
    println("Wrote to file.");
  }

}