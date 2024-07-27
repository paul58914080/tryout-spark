package edu.sample.spark.core.reduce

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

class ReduceByKey {
  val configuration: SparkConf = SparkConf().setAppName("Reduce").setMaster("local[*]")
  val sc: JavaSparkContext = JavaSparkContext(configuration)

  fun countLogType(inputData: List<String>): List<Tuple2<String, Long>> {
    return sc.use {
      it
        .parallelize(inputData)
        .mapToPair { Tuple2(it.split(":")[0], 1L) }
        .reduceByKey { a, b -> a + b }
        .collect()
    }
  }
}
