package edu.kotlin.spark.map

import org.junit.jupiter.api.Test

class MappingTest {

  @Test fun `use map to compute the sqrt of a number`() {
    // Given
    val inputData = listOf(1, 2, 3, 4, 5)
    val mapping = Mapping()
    // When
    val result = mapping.computeSqrt(inputData)
    // Then
    assert(result == listOf(1.0, 1.4142135623730951, 1.7320508075688772, 2.0, 2.23606797749979))
  }
}
