package edu.sample.spark.core.map

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class FlatMapTest {
  @Test
  fun `get the distinct words by flatMap`() {
    // Given
    val inputData =
      listOf(
        "WARN: first warning",
        "ERROR: first error",
        "WARN: second warning",
        "ERROR: second error",
        "WARN: third warning",
      )
    val flatMap = FlatMap()
    // When
    val words: List<String> = flatMap.getWords(inputData)
    // Then
    Assertions.assertThat(words)
      .hasSize(7)
      .containsAll(listOf("WARN:", "ERROR:", "first", "second", "third", "warning", "error"))
  }
}
