import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AlsMLlib {

  val ex = "ex_ots_all"
  val windowSize = 5
  val totDays = 17
  val blLength = 2000
  val lambda = 1.0
  val rank = 5
  val numIterations = 10
  

  def main(args: Array[String]) {

//    println(ipToInt("089.098.123"))
//    println(intToIp(ipToInt("089.098.123")))
//
//    return

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Als").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)

    for (startDate <- 1 to totDays - windowSize) {

      // Load and parse the data
      val data = sc.textFile("/media/anindya/New Volume/Datasets/DShield Logs/%s/ratings_%s".format(ex, startDate + windowSize))
      val ratings = data.map(_.split(',') match {
        case Array(victim, attacker, rate) =>
          Rating(ipToInt(attacker), victim.toInt, rate.toDouble)
      })

      // Build the recommendation model using ALS

      val model = ALS.train(ratings, rank, numIterations, lambda)

      // Evaluate the model on rating data
//      val attackerVictims = ratings.map {
//        case Rating(attacker, victim, rate) =>
//          (attacker, victim)
//      }
      
      val attackers = ratings.map {
        case Rating(attacker, victim, rate) =>
          attacker
      }
      
      
      val victims = ratings.map {
        case Rating(attacker, victim, rate) =>
          victim
      }
      
      val attackerVictims = attackers.distinct().cartesian(victims.distinct()) 

      val predictions =
        model.predict(attackerVictims).map {
          case Rating(attacker, victim, rate) =>
            (victim, (intToIp(attacker), rate))
        }
      
      val alsPred = predictions
      .groupByKey()
      .sortByKey()
      .flatMap { x =>
        var sortedAttackers = x._2.toSeq.sortWith { (x, y) =>
          if (x._2 > y._2) true
          else false
        }
        for (attacker <- sortedAttackers.take(blLength)) yield (x._1, attacker._1, attacker._2)
      }
      
      alsPred.map { x => x._1 + "," + x._2 + "," + x._3 }
          .saveAsTextFile("/media/anindya/New Volume/Datasets/DShield Logs/%s/als_ratings_%s".format(ex, startDate + windowSize))
    }
  }

  def ipToInt(ip: String): Int = {
    ("1" + ip.replaceAll("\\.", "")).toInt
  }

  def intToIp(num: Int): String = {
    num.toString().substring(1).grouped(3).mkString(".")
  }

}