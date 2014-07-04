package de.unimannheim.dws.preprocessing

import scala.io.BufferedSource
import scala.io.Source
import de.unimannheim.dws.models.mongo.CommonLogFile
import scala.util.matching.Regex
import org.joda.time.DateTime
import java.io.File
import java.net.URLDecoder
import scala.util.Try

// http://sujitpal.blogspot.de/2009/06/some-access-log-parsers.html
// http://url-encoder.de/	

object LogFileParser {
  def readFile(path: String): BufferedSource = {
    Source.fromFile(new File(path))
  }

  def parse(format: List[Regex], line: String): CommonLogFile = {

    /*
     * Recursively apply the sequence of regex patterns to the line 
     */
    val result = parseRec(format, List(), line)

    /*
     * Generate key-value pairs for all elements of the actual request
     */
    val requestElems = result(7).trim()

    if (requestElems.length > 8) {

      val requestTupleList = for {
        elem <- requestElems.split("&")
      } yield {
        //tuple._1 -> tuple._2
        val requestTuple = elem.split("=", 2)
        requestTuple.length match {
          case 2 => {
            val key = requestTuple(0).contains("/sparql?") match {
              case true => requestTuple(0).replace("/sparql?", "")
              case _ => requestTuple(0)
            }
            if (key.contains(".")) ("error", "error")
            else {
              val valueUnicodeRemoved = new Regex("%u\\w{4}").replaceAllIn(requestTuple(1), "")
              try {
                val value = URLDecoder.decode(valueUnicodeRemoved)
                (key, value)
              } catch {
                case iae: java.lang.IllegalArgumentException => ("error", "error")
              }
            }
          }
          case _ => ("error", "error")
        }
      }

      /*
     * Return newly instantiated LogFile object
     */
      CommonLogFile(ip = result(0).trim(),
        rfc921 = result(1).trim(),
        username = result(2).trim(),
        date = result(3).trim(),
        time = result(4).trim(),
        timezone = result(5).trim(),
        requestMethod = result(6).trim(),
        request = Map(requestTupleList: _*),
        requestProtocol = result(8).trim(),
        httpStatus = result(9).trim())
    } else
      null
    //"2d0f7e6a4bd6ceebebddfa06b0d1da63", userIdentifier = "-", timestamp = DateTime.now, request = "GET /sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=SELECT+%3Fg+COUNT%28*%29+%7B+GRAPH+%3Fg+%7B%3Fs+%3Fp+%3Fo.%7D+%7D+GROUP+BY+%3Fg+ORDER+BY+DESC+2%3B%0D%0A&format=text%2Fhtml&timeout=30000&debug=on HTTP/1.1", httpStatus = "200", size = "0", referrer = "-", agent = "-")
  }

  def parseRec(format: List[Regex], result: List[String], line: String): List[String] = {

    format match {
      case regex :: tail => {
        regex.findFirstMatchIn(line) match {
          case Some(res) => {
            parseRec(tail, result :+ res.group(1), regex replaceFirstIn (line, ""))
          }
          case None => parseRec(tail, result :+ "", line)
        }
      }
      case Nil => {
        result
      }
    }
  }
}