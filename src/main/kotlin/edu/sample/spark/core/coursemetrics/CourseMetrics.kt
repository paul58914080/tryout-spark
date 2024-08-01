package edu.sample.spark.core.coursemetrics

import java.io.Serializable
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

class CourseMetrics : Serializable {

  fun computeScoreForEachCourse(jsc: JavaSparkContext): JavaPairRDD<String, Long> =
    getCourseWithViewPercentagePairRDD(jsc)
      .mapValues { getScore(it) } // (courseId, score)
      .reduceByKey(Long::plus) // (courseId, score)
      .join(getTitles(jsc)) // (courseId, (score, title))
      .mapToPair { Tuple2(it._2._1, it._2._2) } // (score, title)
      .sortByKey() // (score, title)
      .mapToPair { Tuple2(it._2, it._1) } // (title, score)

  private fun getTitles(jsc: JavaSparkContext): JavaPairRDD<Long, String> =
    jsc.textFile("src/main/resources/viewing_figures/titles.csv").mapToPair { row ->
      val cols = row.split(",")
      Tuple2(cols[0].toLong(), cols[1]) // (courseId, title)
    }

  private fun getCourseWithViewPercentagePairRDD(jsc: JavaSparkContext): JavaPairRDD<Long, Double> =
    getCourseWithViewsAndTotalChapterPairRDD(jsc).mapValues { it._1.toDouble() / it._2 }

  private fun getCourseWithViewsAndTotalChapterPairRDD(
    jsc: JavaSparkContext
  ): JavaPairRDD<Long, Tuple2<Long, Long>> =
    getCourseViewCountPairRDD(jsc).join(getCourseTotalChapterCountPairRDD(jsc))

  fun getCourseTotalChapterCountPairRDD(jsp: JavaSparkContext): JavaPairRDD<Long, Long> {
    val courseChapterCountAs1PairRDD = getCourseChapterAs1PairRDD(jsp)
    return courseChapterCountAs1PairRDD.reduceByKey { a, b ->
      a + b
    } // (courseId, totalNumberOfChapters)
  }

  fun getCourseViewCountPairRDD(jsc: JavaSparkContext): JavaPairRDD<Long, Long> {
    val views = jsc.textFile("src/main/resources/viewing_figures/views-*.csv")
    val chapterAndUserMapToPair =
      views
        .mapToPair { row ->
          val cols = row.split(",")
          Tuple2(cols[1].toLong(), cols[0].toLong()) // (chapterId, userId)
        }
        .distinct()
    val chapters = jsc.textFile("src/main/resources/viewing_figures/chapters.csv")
    val chapterAndCoursePairRDD =
      chapters.mapToPair { row ->
        val cols = row.split(",")
        Tuple2(cols[0].toLong(), cols[1].toLong()) // (chapterId, courseId)
      }

    return chapterAndUserMapToPair
      .join(chapterAndCoursePairRDD) // (chapterId, (userId, courseId))
      .mapToPair { Tuple2(it._2, 1L) } // ((userId, courseId), 1)
      .reduceByKey { a, b -> a + b } // ((userId, courseId), viewCount)
      .mapToPair { Tuple2(it._1._2, it._2) } // (courseId, viewCount)
  }

  private fun getCourseChapterAs1PairRDD(jsp: JavaSparkContext): JavaPairRDD<Long, Long> =
    jsp.textFile("src/main/resources/viewing_figures/chapters.csv").mapToPair { row ->
      val cols = row.split(",")
      Tuple2(cols[1].toLong(), 1L) // (courseId, 1)
    }

  private fun getScore(percentage: Double): Long =
    when {
      percentage > 0.9 -> 10L
      percentage > 0.5 -> 4L
      percentage > 0.25 -> 2L
      else -> 0L
    }
}
