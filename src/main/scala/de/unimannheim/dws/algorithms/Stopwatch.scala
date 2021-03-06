package de.unimannheim.dws.algorithms

class Stopwatch {
  var startTime = System.currentTimeMillis
  var elapsedTime: Long = 0
 
  def start = { startTime = System.currentTimeMillis; elapsedTime = 0 }
  def stop = { elapsedTime = System.currentTimeMillis - startTime }
   
  override def toString = "elapsed time: " + elapsedTime + " milliseconds"
}
 
object Stopwatch {
 
  def apply(): Stopwatch = new Stopwatch
 
  def time(tag: String)(f: => Unit): Stopwatch = {
    val sw = Stopwatch()
    f
    sw.stop
    sw
  }
}