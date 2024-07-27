package edu.sample.spark.core.reduce

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

class Reduce {
  val configuration: SparkConf = SparkConf().setAppName("Reduce").setMaster("local[*]")
  val sc: JavaSparkContext = JavaSparkContext(configuration)

  fun addAllElements(inputData: List<Int>): Int {
    return sc.use { it.parallelize(inputData).reduce { a, b -> a + b } }
  }
}
