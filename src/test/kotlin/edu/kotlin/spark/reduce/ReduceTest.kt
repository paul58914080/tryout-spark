package edu.kotlin.spark.reduce

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ReduceTest {

  @Test
  fun `test reduce function`() {
    // Given
    val inputData = listOf(1, 2, 3, 4, 5)
    val reduce = Reduce()
    // When
    val result = reduce.addAllElements(inputData)
    // Then
    assertThat(result).isEqualTo(15)
  }
}
