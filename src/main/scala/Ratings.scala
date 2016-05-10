import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import java.io.PrintStream

object Project {
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IP Blacklisting").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    var startDate = 2
    var windowSize = 5
    var alpha = 0.8

    for (startDate <- 1 to 11) {
      
      var ps = new PrintStream("/media/anindya/New Volume/Datasets/DShield Logs/stat_" + (startDate + windowSize))

      var logs = sc.parallelize(new Array[String](0))
      for (i <- startDate to startDate + windowSize - 1) {
        logs = logs.union(sc.textFile("/media/anindya/New Volume/Datasets/DShield Logs/" + i))
      }

      val avd = logs.map { x => x.split(",") }
        .filter { x => x.length == 12 }
        .map { x =>
          x.map { x =>
            if (x.length() == 2) ""
            else x.substring(1, x.length() - 1)
          }
        }
        .map { x => ((x(2).toInt, getIpPrefix(x(4))), x(0)) }
        .distinct

      val maxDate = avd.map(x => x._2).distinct().collect().maxBy { x => x }

      val ratings = avd.groupByKey().map { x =>
        (x._1, x._2.map { x => alpha * Math.pow((1 - alpha), Util.dayDiff(x, maxDate)) }.reduce((x, y) => x + y))
      }
        .map { x => (x._1._1, (x._1._2, x._2)) }
        .groupByKey()
        .sortByKey()
        .flatMap { x =>
          var sortedAttackers = x._2.toSeq.sortWith { (x, y) =>
            if (x._2 > y._2) true
            else false
          }
          for (attacker <- sortedAttackers.take(2000)) yield (x._1, attacker._1, attacker._2)
        }

      ratings.map { x => x._1 + "," + x._2 + "," + x._3 }
        .saveAsTextFile("/media/anindya/New Volume/Datasets/DShield Logs/ex2/predicted_ratings_" + (startDate + windowSize))

      val numVictims = ratings.map { x => x._1 }.distinct().count()
      val numAttackers = ratings.map { x => x._2 }.distinct().count()
      val numRatings = ratings.count()

      ps.println("Num of attackers: " + numAttackers)
      ps.println("Num of victims: " + numVictims)
      ps.println("Num of ratings: " + numRatings)
      ps.println("Matrix size: " + numAttackers * numVictims)
      ps.println("Matrix density: " + (numRatings * 1.0) / (numAttackers * numVictims))
    }

  }

  def getIpPrefix(ip: String): String = {
    ip.substring(0, ip.lastIndexOf("."))
  }

}