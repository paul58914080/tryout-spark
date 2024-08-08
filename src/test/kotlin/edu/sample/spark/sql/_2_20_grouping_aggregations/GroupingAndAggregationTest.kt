package edu.sample.spark.sql._2_20_grouping_aggregations

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class GroupingAndAggregationTest {

  @Test
  fun `count the number of logs for each level`() {
    // Given
    val groupingAndAggregation = GroupingAndAggregation()
    // When
    val countByLevel: Map<String, Long> = groupingAndAggregation.countLogsByLevel()
    // Then
    assertThat(countByLevel)
      .containsExactlyInAnyOrderEntriesOf(mapOf("WARN" to 2, "FATAL" to 2, "INFO" to 1))
  }
}
