package edu.sample.spark.core.coursemetrics

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import scala.Tuple2

class CourseMetricsTest {
  @Test
  fun `compute the number of chapters for a given course`() {
    // Given
    val courseMetrics = CourseMetrics()
    // When
    val courseAndChapterCount: List<Tuple2<Long, Long>> = courseMetrics.getCourseChapterCount()
    // Then
    assertThat(courseAndChapterCount)
      .containsAnyOf(Tuple2(1L, 34L))
  }
}
