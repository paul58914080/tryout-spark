package edu.kotlin.spark

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

fun main() {
  val inputData = listOf(1, 2, 3, 4, 5)

  val configuration = SparkConf().setAppName("Hello World").setMaster("local[*]")
  val sc = JavaSparkContext(configuration)
  sc.use { it.parallelize(inputData).map { it * 2 }.collect().forEach(::println) }
}
