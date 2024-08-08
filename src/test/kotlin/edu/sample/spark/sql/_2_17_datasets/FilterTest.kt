package edu.sample.spark.sql._2_17_datasets

import org.apache.spark.sql.SparkSession
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class FilterTest {
  val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()
  val studentsDataset =
    sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")

  @Test
  fun `the first record subject`() {
    // Given
    val filter = Filter()
    // When
    val subject: String = filter.firstRecordSubject()
    // Then
    assertThat(subject).isEqualTo("Math")
    // When
    val year = filter.firstRecordYear()
    // Then
    assertThat(year).isEqualTo(2005)
  }

  @Test
  fun `count the records for a particular subject with 'sql' like filters`() {
    // Given
    val filter = Filter()
    // When
    val count: Long = filter.countRecordsForSubjectWithSqlFilters("Modern Art")
    // Then
    assertThat(count).isEqualTo(149_980L)
  }

  @Test
  fun `count the records for a particular subject and year with 'java lambda' like filters`() {
    // Given
    val filter = Filter()
    // When
    val count: Long = filter.countRecordsForSubjectAndYearWithJavaLambdaFilters("Modern Art", 2005)
    // Then
    assertThat(count).isEqualTo(15_764L)
  }

  @Test
  fun `count the records for a particular subject and year with 'dataset column' filters`() {
    // Given
    val filter = Filter()
    // When
    val count: Long = filter.countRecordsForSubjectAndYearWithColFilters("Modern Art", 2005)
    // Then
    assertThat(count).isEqualTo(15_764L)
  }

  @Test
  fun `count the records for a particular subject and year with 'function col' filters`() {
    // Given
    val filter = Filter()
    // When
    val count: Long = filter.countRecordsForSubjectAndYearWithFunctionColFilters("Modern Art", 2005)
    // Then
    assertThat(count).isEqualTo(15_764L)
  }

  @Test
  fun `compute the max score for the subject 'French' with 'temp view' like filters`() {
    // Given
    val filter = Filter()
    // When
    val maxScore: Int = filter.maxScoreForSubjectWithTempViewFilters("French")
    // Then
    assertThat(maxScore).isEqualTo(98)
  }

  @Test
  fun `compute the max score for the subject 'Frenchies' which does not exist with 'temp view' like filters`() {
    // Given
    val filter = Filter()
    // When
    val maxScore: Int = filter.maxScoreForSubjectWithTempViewFilters("Frenchies")
    // Then
    assertThat(maxScore).isEqualTo(0)
  }
}
