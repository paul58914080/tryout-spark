package edu.sample.spark.core.joins

import org.apache.spark.api.java.Optional
import org.apache.spark.api.java.Optional.empty
import org.apache.spark.api.java.Optional.of
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import scala.Tuple2

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WebsiteMetricsTest {
  lateinit var visitsStore: List<Visit>
  lateinit var usersStore: List<User>

  @BeforeAll
  internal fun loadData() {
    visitsStore = listOf(Visit(4, 18), Visit(6, 4), Visit(10, 9))
    usersStore =
      listOf(
        User(1, "John"),
        User(2, "Bob"),
        User(3, "Alan"),
        User(4, "Doris"),
        User(5, "Marybelle"),
        User(6, "Raquel"),
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
    assertThat(result)
      .hasSize(2)
      .containsAll(listOf(Tuple2(4, Tuple2(18, "Doris")), Tuple2(6, Tuple2(4, "Raquel"))))
  }

  @Test
  fun `count the number of visits per user by left outer join`() {
    // Given
    val websiteMetrics = WebsiteMetrics(visitsStore, usersStore)
    // When
    val result: List<Tuple2<Long, Tuple2<Long, Optional<String>>>> =
      websiteMetrics.getMetricForAllUsersVisitsButNotAvailableInUserStore()
    // Then
    assertThat(result)
      .hasSize(3)
      .containsAll(
        listOf(
          Tuple2(4, Tuple2(18, of("Doris"))),
          Tuple2(6, Tuple2(4, of("Raquel"))),
          Tuple2(10, Tuple2(9, empty())),
        )
      )
  }

  @Test
  fun `count the number of visits per user by right outer join`() {
    // Given
    val websiteMetrics = WebsiteMetrics(visitsStore, usersStore)
    // When
    val result: List<Tuple2<Long, Tuple2<Optional<Long>, String>>> =
      websiteMetrics.getMetricForAllUsersVisitsButNotAvailableInVisitStore()
    // Then
    assertThat(result)
      .hasSize(6)
      .containsAll(
        listOf(
          Tuple2(1, Tuple2(empty(), "John")),
          Tuple2(2, Tuple2(empty(), "Bob")),
          Tuple2(3, Tuple2(empty(), "Alan")),
          Tuple2(4, Tuple2(of(18), "Doris")),
          Tuple2(5, Tuple2(empty(), "Marybelle")),
          Tuple2(6, Tuple2(of(4), "Raquel")),
        )
      )
  }

  @Test
  fun `count the number of visits per user by full outer join`() {
    // Given
    val websiteMetrics = WebsiteMetrics(visitsStore, usersStore)
    // When
    val result: List<Tuple2<Long, Tuple2<Optional<Long>, Optional<String>>>> =
      websiteMetrics.getMetricForAllUsersVisitsButNotAvailableInUserOrVisitStore()
    // Then
    assertThat(result)
      .hasSize(7)
      .containsAll(
        listOf(
          Tuple2(1, Tuple2(empty(), of("John"))),
          Tuple2(2, Tuple2(empty(), of("Bob"))),
          Tuple2(3, Tuple2(empty(), of("Alan"))),
          Tuple2(4, Tuple2(of(18), of("Doris"))),
          Tuple2(5, Tuple2(empty(), of("Marybelle"))),
          Tuple2(6, Tuple2(of(4), of("Raquel"))),
          Tuple2(10, Tuple2(of(9), empty())),
        )
      )
  }
}
