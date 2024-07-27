package edu.sample.spark.core.tuple

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import scala.Tuple2

class SparkTupleTest {

  @Test
  fun `print number and its sqrt`() {
    // Given
    val inputData = listOf(1, 2, 3, 4, 5)
    val sparkTuple = SparkTuple()
    // When
    val result: List<Tuple2<Int, Double>> = sparkTuple.computeSqrt(inputData)
    // Then
    Assertions.assertThat(result)
      .containsExactly(
        Tuple2(1, 1.0),
        Tuple2(2, 1.4142135623730951),
        Tuple2(3, 1.7320508075688772),
        Tuple2(4, 2.0),
        Tuple2(5, 2.23606797749979),
      )
  }
}
