import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import breeze.linalg._
import org.apache.spark.HashPartitioner
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.PrintStream
import java.util.Calendar

object Als {
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val startDate = 6
    val windowSize = 5

    val conf = new SparkConf().setAppName("Als").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    
    val time1 = Calendar.getInstance.getTime

    val ratings = sc.textFile("/media/anindya/New Volume/Datasets/DShield Logs/ratings_" + (startDate + windowSize))
    .map{l =>
      val tokens = l.split(",")
      (tokens(0), tokens(1), tokens(2))
    }

    val victims = ratings.map(x => x._1).distinct
    val attackers = ratings.map(x => x._2).distinct

    val victimCount = victims.count
    val attackerCount = attackers.count

    val k = 5

    val victimMatrix = victims.map(x => (x, DenseVector.zeros[Double](k)))
    var myVictimMatrix = victimMatrix.map(x => (x._1, x._2(0 to k - 1) := 0.1)).partitionBy(new HashPartitioner(10)).persist

    val attackerMatrix = attackers.map(x => (x, DenseVector.zeros[Double](k)))
    var myAttackerMatrix = attackerMatrix.map(x => (x._1, x._2(0 to k - 1) := 0.1)).partitionBy(new HashPartitioner(10)).persist

    val ratingByVictim = sc.broadcast(ratings.map(x => (x._1, (x._2, x._3))))
    val ratingByAttacker = sc.broadcast(ratings.map(x => (x._2, (x._1, x._3))))

    val lambda = 1.0
    val regMatrix = DenseMatrix.zeros[Double](k, k)
    
    regMatrix(0, ::) := DenseVector(lambda, 0, 0, 0, 0).t
    regMatrix(1, ::) := DenseVector(0, lambda, 0, 0, 0).t
    regMatrix(2, ::) := DenseVector(0, 0, lambda, 0, 0).t
    regMatrix(3, ::) := DenseVector(0, 0, 0, lambda, 0).t
    regMatrix(4, ::) := DenseVector(0, 0, 0, 0, lambda).t

    for (i <- 1 to 10) {

      myAttackerMatrix = ratingByVictim.value.join(myVictimMatrix).map { x =>
        (x._2._1._1, (x._2._2 * x._2._2.t, x._2._1._2.toDouble * x._2._2))
      }
      .reduceByKey { (x, y) =>
        (x._1 + y._1, x._2 + y._2)
      }
      .map { x =>
        (x._1, inv(x._2._1 + regMatrix) * x._2._2)
      }
        
      myVictimMatrix = ratingByAttacker.value.join(myAttackerMatrix).map { x =>
        (x._2._1._1, (x._2._2 * x._2._2.t, x._2._1._2.toDouble * x._2._2))
      }
      .reduceByKey { (x, y) =>
        (x._1 + y._1, x._2 + y._2)
      }
      .map { x =>
        (x._1, inv(x._2._1 + regMatrix) * x._2._2)
      }

    }
    
    val time2 = Calendar.getInstance.getTime
    
    var cross = myVictimMatrix.cartesian(myAttackerMatrix)
    println("cross product size: " + cross.count())
    
    cross
    .map{ x =>
      (x._1._1, (x._2._1, x._2._2.t * x._1._2))
    }
    .groupByKey()
    .sortByKey()
    .flatMap{ x=>
      var sortedAttackers = x._2.toSeq.sortWith{ (x,y) =>
         if(x._2 > y._2) true
         else false
      }
      for(attacker <- sortedAttackers.take(2500)) yield (x._1, attacker._1, attacker._2)
    }
    .map{ x => x._1 + "," + x._2 + "," + x._3 }
    .saveAsTextFile("/media/anindya/New Volume/Datasets/DShield Logs/predicted_ratings_" + (startDate + windowSize))
    
    val time3 = Calendar.getInstance.getTime
    
    println(time1 + " " + time2 + " " + time3)
    
  }
}