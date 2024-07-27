package edu.sample.spark.core.tuple

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2
import kotlin.math.sqrt

class SparkTuple {
  val configuration: SparkConf = SparkConf().setAppName("SparkTuple").setMaster("local[*]")
  val sc: JavaSparkContext = JavaSparkContext(configuration)

  fun computeSqrt(inputData: List<Int>): List<Tuple2<Int, Double>> {
    return sc.use { it.parallelize(inputData).map { Tuple2(it, sqrt(it.toDouble())) }.collect() }
  }
}
