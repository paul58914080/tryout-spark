package edu.kotlin.spark.map

import org.junit.jupiter.api.Test
import scala.Tuple2

class MapToPairTest {

  @Test
  fun `map to pair the number with its square`() {
    // Given
    val inputData = listOf(1, 2, 3, 4, 5)
    val mapToPair = MapToPair()
    // When
    val result = mapToPair.pairNumberWithSquare(inputData)
    // Then
    assert(result == listOf(Tuple2(1, 1), Tuple2(2, 4), Tuple2(3, 9), Tuple2(4, 16), Tuple2(5, 25)))
  }
}
