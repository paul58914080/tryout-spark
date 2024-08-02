package edu.sample.spark.core.map

import org.junit.jupiter.api.Test

class MappingTest {

  @Test fun `use map to compute the sqrt of a number`() {
    // Given
    val inputData = listOf(1, 2, 3, 4)
    val mapping = Mapping()
    // When
    val result = mapping.computeSqrt(inputData)
    // Then
    assert(result == listOf(1.0, 1.4142135623730951, 1.7320508075688772, 2.0))
  }

  @Test fun `use map and reduce to determine the count of elements`() {
    // Given
    val inputData = listOf(1, 2, 3, 4, 5)
    val mapping = Mapping()
    // When
    val result = mapping.countElements(inputData)
    // Then
    assert(result == 5)
  }
}
