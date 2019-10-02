import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import java.io.PrintStream


object Sanitizer {
  
   var ex = ""
   val blLength = 2000
  

   def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Sanitizer").setMaster("local")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)
    
     for (alpha <- 0.60 to 0.96 by 0.05) {

      ex = "ex_ts_alp_%.2f_%s".format(alpha, blLength)

      var lines = sc.textFile("/media/anindya/New Volume/Datasets/DShield Logs/%s/hitcount".format(ex))
      val ps = new PrintStream("/media/anindya/New Volume/Datasets/DShield Logs/ts_comp/_%.2f.dat".format(alpha))
      
      lines = lines.map{ x =>
        x.replace("Hit-count for day ", "").replace(": ","\t")
      }
      
      for(line <- lines.collect()){
        ps.println(line)
      }
      
      ps.flush()
      ps.close()
      
     }
      
    
   }
}