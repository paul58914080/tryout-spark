package edu.kotlin.spark.map

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

class FlatMap {
  val configuration: SparkConf = SparkConf().setAppName("FlatMap").setMaster("local[*]")
  val sc: JavaSparkContext = JavaSparkContext(configuration)

  fun getWords(inputData: List<String>): List<String> {
    return sc.use {
      it.parallelize(inputData).flatMap { it.split(" ").iterator() }.distinct().collect()
    }
  }
}
