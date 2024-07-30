package edu.sample.spark.core.joins

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import scala.Tuple2

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WebsiteMetricsTest {
  lateinit var visitsStore: List<Tuple2<Long, Long>>
  lateinit var usersStore: List<Tuple2<Long, String>>

  @BeforeAll
  internal fun loadData(): Unit {
    visitsStore = listOf(Tuple2(4, 18), Tuple2(6, 4), Tuple2(10, 9))
    usersStore =
      listOf(
        Tuple2(1, "John"),
        Tuple2(2, "Bob"),
        Tuple2(3, "Alan"),
        Tuple2(4, "Doris"),
        Tuple2(5, "Marybelle"),
        Tuple2(6, "Raquel"),
      )
  }

  @Test
  fun `count the number of visits per user by inner join`() {
    // Given
    val websiteMetrics = WebsiteMetrics(visitsStore, usersStore)
    // When
    val result: List<Tuple2<Long, Tuple2<Long, String>>> =
      websiteMetrics.getMetricOnlyForAvailableUserAndVisit()
    // Then
    Assertions.assertThat(result)
      .hasSize(2)
      .containsAll(listOf(Tuple2(4, Tuple2(18, "Doris")), Tuple2(6, Tuple2(4, "Raquel"))))
  }

  @Test fun `count the number of visits per user by left outer join`() {}

  @Test fun `count the number of visits per user by right outer join`() {}
}
