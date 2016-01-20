/**
 * Created by brian.cajes on 1/20/16.
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.bdgenomics.adam.rdd.ADAMContext

class AdamHelloWorld {

}

object AdamHelloWorld{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val ac = new ADAMContext(context)
    val pathToAdamVCFs = "adam_vcf_example"
    val genotypes = ac.loadGenotypes(pathToAdamVCFs)
    val simpleGenotypeRDD = genotypes.map(g => (g.getSampleId, g.getVariant.getContig.getContigName, g.getVariant.getStart, g.getVariant.getReferenceAllele, g.getVariant.getAlternateAllele, g.getAlleles.get(0).toString, g.getAlleles.get(1).toString))
    simpleGenotypeRDD.collect.foreach(println)
    context.stop
  }
}
