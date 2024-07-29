package edu.sample.spark.core.disk

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class ReadBigDataFileFromDiskTest {

  @Test
  fun `read big data file from disk`() {
    // Given
    val readBigDataFileFromDisk = ReadBigDataFileFromDisk()
    // When
    val result: List<String> = readBigDataFileFromDisk.getSubtitles()
    // Then
    Assertions.assertThat(result).hasSize(44040)
  }
}
