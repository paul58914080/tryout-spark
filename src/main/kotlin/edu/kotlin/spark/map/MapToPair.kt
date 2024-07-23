package edu.kotlin.spark.map

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

class MapToPair {
  val configuration: SparkConf = SparkConf().setAppName("MapPair").setMaster("local[*]")
  val sc: JavaSparkContext = JavaSparkContext(configuration)

  fun pairNumberWithSquare(inputData: List<Int>): List<Tuple2<Int, Int>> {
    return sc.use { it.parallelize(inputData).mapToPair { Tuple2(it, it * it) }.collect() }
  }
}
