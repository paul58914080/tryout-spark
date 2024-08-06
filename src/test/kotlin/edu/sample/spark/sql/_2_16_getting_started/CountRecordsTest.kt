package edu.sample.spark.sql._2_16_getting_started

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CountRecordsTest {

  @Test
  fun `count the number of students records`() {
    // Given
    val countRecords = CountRecords()
    // When
    val count: Long = countRecords.countStudentRecords()
    // Then
    assertThat(count).isEqualTo(20_80_223L)
  }
}
