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
}
