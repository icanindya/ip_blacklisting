import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import java.io.PrintStream

object Statistics {
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IP Blacklisting").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)

    var lines = 0L
    var attackers = 0L
    var victims = 0L
    var attackersSub = 0L
    
    for (i <- 1 to 17) {
      
      val logs = sc.textFile("/media/anindya/New Volume/Datasets/DShield Logs/" + i)
      val av = logs.map { x => x.split(",") }
        .filter { x => x.length == 12 }
        .map { x =>
          x.map { x =>
            if (x.length() == 2) ""
            else x.substring(1, x.length() - 1)
          }
        }
        .map { x => (x(2), x(4))}
        .distinct
        
        victims += av.map{ x=>
          x._1
        }.distinct.count()
        
        attackers += av.map{ x=>
          x._2
        }.distinct.count()
      
        attackersSub += av.map{ x=>
          getIpPrefix(x._2)
        }.distinct.count()
        
    }
    
     
    
    println(attackers + " " + victims + " " + attackersSub)
    
  }
  
   def getIpPrefix(ip: String): String = {
    ip.substring(0, ip.lastIndexOf("."))
  }
}