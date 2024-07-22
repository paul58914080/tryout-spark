package edu.kotlin.spark.map

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

class Mapping {

  val configuration: SparkConf = SparkConf().setAppName("Map").setMaster("local[*]")
  val sc: JavaSparkContext = JavaSparkContext(configuration)

  fun computeSqrt(inputData: List<Int>): List<Double> {
    return sc.use { it.parallelize(inputData).map { Math.sqrt(it.toDouble()) }.collect() }
  }

  fun countElements(inputData: List<Int>): Int {
    return sc.use { it.parallelize(inputData).map { 1 }.reduce { a, b -> a + b } }
  }
}
