package edu.sample.spark.core.partitions

import edu.sample.spark.util.WaitForDAG
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

class Partitions {

  fun getLogsByTypeAndDateTimeAsJavaPairRDD(
    filepath: String,
    javaSparkContext: JavaSparkContext,
  ): JavaPairRDD<LogType, String> {
    return javaSparkContext.textFile(filepath).mapToPair { row ->
      val cols = row.split(",")
      val logType = LogType.valueOf(cols[0])
      val dateTime = cols[1]
      Tuple2(logType, dateTime)
    }
  }

  fun printForEachLogTypeTheCountOfMessages(javaSparkContext: JavaSparkContext): Unit {
    getLogsByTypeAndDateTimeAsJavaPairRDD("src/main/resources/big_log/biglog_without_header.txt", javaSparkContext)
      .groupByKey()
      .mapValues { it.count() }
      .collect()
      .forEach { println("${it._1} has ${it._2} elements") }
  }
}

fun main() {
  val partitions = Partitions()
  val configuration = SparkConf().setAppName("Partitions").setMaster("local[*]")
  val sc = JavaSparkContext(configuration)
  sc.use {
    partitions.printForEachLogTypeTheCountOfMessages(it)

    // This code snippet is used to keep the Spark UI open
    // http://localhost:4040/jobs/
    WaitForDAG().pause()
  }
}

enum class LogType {
  FATAL,
  ERROR,
  WARN,
  INFO,
  DEBUG,
  TRACE,
}
