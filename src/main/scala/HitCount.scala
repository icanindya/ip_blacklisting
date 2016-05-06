import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import java.io.PrintStream

object HitCount {
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IP Blacklisting").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    var windowSize = 5

     val ps = new PrintStream("/media/anindya/New Volume/Datasets/DShield Logs/hitcount")
    
    for (startDate <- 1 to 6) {
      
      val logs = sc.textFile("/media/anindya/New Volume/Datasets/DShield Logs/" + (startDate + windowSize))

      val trueAVPA = logs.map { x => x.split(",") }
        .filter { x => x.length == 12 }
        .map { x =>
          x.map { x =>
            if (x.length() == 2) ""
            else x.substring(1, x.length() - 1)
          }
        }
        .map { x => (x(2).toInt, x(4), x(0)) }
        .distinct()
        
      println("Total attackers: " + trueAVPA.count())  
   
        
      val trueAVP =   trueAVPA
        .map{ x => ((x._1, getIpPrefix(x._2)), x._3) }
        
        
      println("Total attackers subnet: " + trueAVP.distinct().count())  

      val predicts = sc.textFile("/media/anindya/New Volume/Datasets/DShield Logs/predicted_ratings_" + (startDate + windowSize))

      val predictedAVP = predicts.map { x =>
        val tokens = x.split(",")
        ((tokens(0).toInt, tokens(1)), tokens(2))
      }

      val hitCount = predictedAVP.join(trueAVP).count()

      println(trueAVP.count())
      println(predictedAVP.count())
      println("Hit-count for day " + (startDate + windowSize) + ": " + hitCount)
      ps.println("Hit-count for day " + (startDate + windowSize) + ": " + hitCount)
      ps.flush()
   
    }
    
    ps.close

  }
  
  def getIpPrefix(ip: String): String = {
    ip.substring(0, ip.lastIndexOf("."))
  }
}