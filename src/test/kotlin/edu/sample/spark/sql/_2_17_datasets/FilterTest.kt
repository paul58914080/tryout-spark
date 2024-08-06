package edu.sample.spark.sql._2_17_datasets

import org.apache.spark.sql.SparkSession
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class FilterTest {
  val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()

  @Test
  fun `the first record subject`() {
    sparkSession.use {
      // Given
      val filter = Filter()
      // When
      val subject: String = filter.firstRecordSubject(sparkSession)
      // Then
      assertThat(subject).isEqualTo("Math")
      // When
      val year = filter.firstRecordYear(sparkSession)
      // Then
      assertThat(year).isEqualTo(2005)
    }
  }

  @Test
  fun `count the records for a particular subject`() {
    // Given
    val filter = Filter()
    // When
    val count: Long = filter.countRecordsForSubject(sparkSession, "Modern Art")
    // Then
    assertThat(count).isEqualTo(149_980L)
  }

  @Test
  fun `count the records for a particular subject and year`() {
    // Given
    val filter = Filter()
    // When
    val count: Long = filter.countRecordsForSubjectAndYear(sparkSession, "Modern Art", 2005)
    // Then
    assertThat(count).isEqualTo(15_764L)
  }

  @Test
  fun `count the records for a particular subject and year with column filters`() {
    // Given
    val filter = Filter()
    // When
    val count: Long = filter.countRecordsBySubjectAndYearWithColFilters(sparkSession, "Modern Art", 2005)
    // Then
    assertThat(count).isEqualTo(15_764L)
  }
}
