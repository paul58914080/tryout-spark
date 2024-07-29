package edu.sample.spark.core.disk

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

class ReadBigDataFileFromDisk {
  val configuration: SparkConf =
    SparkConf().setAppName("ReadBigDataFileFromDisk").setMaster("local[*]")
  val sc: JavaSparkContext = JavaSparkContext(configuration)

  fun getSubtitles(): List<String> {
    sc.use {
      return it.textFile("src/main/resources/subtitles/input.txt").collect().toList()
    }
  }
}
