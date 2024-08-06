package edu.sample.spark.sql._2_16_getting_started

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CountStudentsTest {

  @Test
  fun `count the number of students records`() {
    // Given
    val countStudents = CountStudents()
    // When
    val count: Long = countStudents.countStudentRecords()
    // Then
    assertThat(count).isEqualTo(20_80_223L)
  }
}
