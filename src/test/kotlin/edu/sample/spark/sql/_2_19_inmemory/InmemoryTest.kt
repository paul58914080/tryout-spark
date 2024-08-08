package edu.sample.spark.sql._2_19_inmemory

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class InmemoryTest {

  @Test
  fun `create a data frame from a list of objects using 'Struct Type'`() {
    // Given
    val inmemory = Inmemory()
    // When
    val count: Long = inmemory.createDataFrameFromListHaving4ItemsOfLogsAndGetCount()
    // Then
    assertThat(count).isEqualTo(4)
  }
}
