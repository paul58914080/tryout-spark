package edu.sample.spark.core.keywordranking

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class KeywordsRankingTest {
  @Test
  fun `determine all the keywords for all the courses apart from boring words`() {
    // Given
    val keywordRanking = KeywordRanking()
    // When
    val boringWords = keywordRanking.getBoringWords()
    val keywords = keywordRanking.getTopNKeywords(100)
    // Then
    assertThat(boringWords).containsAnyOf("shouldnt", "worrying", "simplify", "tidy")
    assertThat(keywords).doesNotContainAnyElementsOf(boringWords)
  }
}
