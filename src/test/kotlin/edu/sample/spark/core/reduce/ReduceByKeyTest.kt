package edu.sample.spark.core.reduce

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import scala.Tuple2

class ReduceByKeyTest {
  @Test
  fun `use reduceByKey to count the number of logs by their log type`() {
    // Given
    val inputData =
      listOf(
        "WARN: first warning",
        "ERROR: first error",
        "WARN: second warning",
        "ERROR: second error",
        "WARN: third warning",
      )
    val reduceByKey = ReduceByKey()
    // When
    val result = reduceByKey.countLogType(inputData)
    // Then
    assertThat(result).containsAll(listOf(Tuple2("WARN", 3), Tuple2("ERROR", 2)))
  }
}
