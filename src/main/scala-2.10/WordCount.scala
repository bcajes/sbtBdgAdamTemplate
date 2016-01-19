/**
 * Created by brian.cajes on 1/19/16.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
 * @author Administrator
 */
class WordCount {

}
object WordCount{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val textFile : RDD[String] = context.textFile("build.sbt", 1)
    val tokenized = textFile.flatMap(_.split(" "))
    val wordCounts = tokenized.map((_, 1))
    val count = wordCounts.reduceByKey(_ + _)
    println(count.collect.mkString(", "));
    context.stop
  }
}