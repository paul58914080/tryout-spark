package edu.sample.spark.core.coursemetrics

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import scala.Tuple2

class CourseMetricsTest {
  val configuration: SparkConf = SparkConf().setAppName("CourseMetricsTest").setMaster("local[*]")
  val sc = JavaSparkContext(configuration)

  @Test
  fun `compute the number of chapters for a given course`() {
    sc.use {
      // Given
      val courseMetrics = CourseMetrics()
      // When
      val courseAndChapterCount: List<Tuple2<Long, Long>> =
        courseMetrics.getCourseTotalChapterCountPairRDD(it).collect() // (courseId, totalNumberOfChapters)
      // Then
      assertThat(courseAndChapterCount).containsAnyOf(Tuple2(1L, 34L))
    }
  }

  @Test
  fun `compute the number of views for a given course`() {
    sc.use {
      // Given
      val courseMetrics = CourseMetrics()
      // When
      val courseAndViewCount: List<Tuple2<Long, Long>> =
        courseMetrics.getCourseViewCountPairRDD(it).collect() // (courseId, viewCount)
      // Then
      assertThat(courseAndViewCount).hasSize(10412).containsAnyOf(Tuple2(1, 10))
    }
  }

  @Test
  fun `compute score for each course sorted by score`() {
    sc.use {
      // Given
      val courseMetrics = CourseMetrics()
      // When
      val scoreForEachCourse: List<Tuple2<String, Long>> =
        courseMetrics.computeScoreForEachCourse(it).collect() // (title, score)
      // Then
      assertThat(scoreForEachCourse)
        .hasSize(18)
        .containsSequence(
          listOf(
            Tuple2("HTML5", 716),
            Tuple2("Java Fundamentals", 2044),
            Tuple2("Wildfly 1", 3188),
            Tuple2("Wildfly 2", 3350),
            Tuple2("Spring Security Module 2", 3678),
            Tuple2("Spring Security Module 3", 4004),
            Tuple2("Java Build Tools", 4830),
            Tuple2("Spring Boot", 5384),
            Tuple2("Android 1", 5416),
            Tuple2("Thymeleaf", 5460),
            Tuple2("NoSQL", 5508),
            Tuple2("Cloud Deployment", 5616),
            Tuple2("Spring Framework Fundmentals", 5616),
            Tuple2("Spring Security Module 1 ", 5856),
            Tuple2("SpringMVC", 5856),
            Tuple2("Spring Remoting and Webservices", 5940),
            Tuple2("Microservice Deployment", 5940),
            Tuple2("Spring Boot Microservices", 5964),
          )
        )
    }
  }
}
