package edu.sample.spark.core.coursemetrics

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

class CourseMetrics {

  val sc: JavaSparkContext

  init {
    val configuration: SparkConf = SparkConf().setAppName("CourseMetrics").setMaster("local[*]")
    sc = JavaSparkContext(configuration)
  }

  fun getCourseChapterCount(): List<Tuple2<Long, Long>> {
    sc.use {
      val titlesPairRDD = getTitlesPairRDD(it)
      val courseChapterCountPairRDD = getCourseChapterCountPairRDD(it)
      return courseChapterCountPairRDD
        .reduceByKey { a, b -> a + b } // (courseId, totalNumberOfChapters)
        .collect()
    }
  }

  private fun getCourseChapterCountPairRDD(jpc: JavaSparkContext): JavaPairRDD<Long, Long> =
    jpc.textFile("src/main/resources/viewing_figures/chapters.csv").mapToPair { row ->
      val cols = row.split(",")
      Tuple2(cols[1].toLong(), 1L)
    }

  private fun getTitlesPairRDD(jpc: JavaSparkContext): JavaPairRDD<Long, String> =
    jpc.textFile("src/main/resources/viewing_figures/titles.csv").mapToPair { row ->
      val cols = row.split(",")
      Tuple2(cols[0].toLong(), cols[1])
    }
}
