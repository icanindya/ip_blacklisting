import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import java.io.PrintStream
import java.io.PrintWriter
import java.io.File

object RatingsTS {

  var ex = "ex"
  val totDays = 17
  val windowSize = 5
  val alpha = 0.9
  val blLength = 2000

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IP Blacklisting").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)


    for (alpha <- 0.60 to 0.96 by 0.05) {
      
      ex = "ex_ts_alp_%.2f_%s".format(alpha, blLength)
      
      val dir = new File("/media/anindya/New Volume/Datasets/DShield Logs/%s".format(ex))
      dir.mkdir()
      
      var ps = new PrintWriter("/media/anindya/New Volume/Datasets/DShield Logs/%s/description.txt".format(ex))
      ps.println("Time-series model with alpha %.2f and blacklist length %d".format(alpha, blLength))
      ps.close()
      
      for (startDate <- 1 to totDays - windowSize) {

        ps = new PrintWriter("/media/anindya/New Volume/Datasets/DShield Logs/%s/stat_%s".format(ex, startDate + windowSize))

        //create empty logs rdd
        var logs = sc.parallelize(new Array[String](0))
        //combine logs from start-date to end-date derived by window size 
        for (i <- startDate to startDate + windowSize - 1) {
          logs = logs.union(sc.textFile("/media/anindya/New Volume/Datasets/DShield Logs/%d".format(i)))
        }

        val avd = logs.map { x => x.split(",") }
          //filter lines having exactly 12 tokens
          .filter { x => x.length == 12 }
          //trim surrounding double quotes from each token
          .map { x =>
            x.map { x =>
              if (x.length() == 2) ""
              else x.substring(1, x.length() - 1)
            }
          }
          //convert line to contributor, attacker, date
          .map { x => ((x(2).toInt, getIpPrefix(x(4))), x(0)) }
          //take unique contributor, attacker, date triples
          .distinct

        val maxDate = avd.map(x => x._2).distinct().collect().maxBy { x => x }

        val ratings = avd.groupByKey().map { x =>
          //rating = 
          (x._1, x._2.map { x => alpha * Math.pow((1 - alpha), Util.dayDiff(x, maxDate))}.reduce((x, y) => x + y))
        }
          .map { x => (x._1._1, (x._1._2, x._2)) }
          .groupByKey()
          .sortByKey()
          .flatMap { x =>
            var sortedAttackers = x._2.toSeq.sortWith { (x, y) =>
              if (x._2 > y._2) true
              else false
            }
            for (attacker <- sortedAttackers.take(blLength)) yield (x._1, attacker._1, attacker._2)
          }

        ratings.map { x => x._1 + "," + x._2 + "," + x._3 }
          .saveAsTextFile("/media/anindya/New Volume/Datasets/DShield Logs/%s/ratings_%s".format(ex, startDate + windowSize))

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
  }

  def getIpPrefix(ip: String): String = {
    ip.substring(0, ip.lastIndexOf("."))
  }

}