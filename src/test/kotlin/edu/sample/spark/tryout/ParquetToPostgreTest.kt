package edu.sample.spark.tryout

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement

class ParquetToPostgreTest {

  companion object {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var connection: Connection

    @BeforeAll
    @JvmStatic
    fun setUp() {
      embeddedPostgres = EmbeddedPostgres.builder().start()
      connection = DriverManager.getConnection(embeddedPostgres.getJdbcUrl("postgres", "postgres"))
      createTable()
    }

    @AfterAll
    @JvmStatic
    fun tearDown() {
      connection.close()
      embeddedPostgres.close()
    }

    private fun createTable() {
      val statement: Statement = connection.createStatement()
      statement.execute(
        """
        CREATE TABLE FLIGHTS (
            FL_DATE TEXT,
            DEP_DELAY TEXT,
            ARR_DELAY TEXT,
            AIR_TIME TEXT,
            DISTANCE TEXT,
            DEP_TIME TEXT,
            ARR_TIME TEXT
        )
        """.trimIndent()
      )
      statement.close()
    }
  }

  @Test
  fun `test persistParquetToPostgres`() {
    // given
    val parquetToPostgre = ParquetToPostgre()
    val pathToParquet =
      ParquetToPostgreTest.javaClass.classLoader.getResource("parquet/flights-1m.parquet").path
    val jdbcUrl = embeddedPostgres.getJdbcUrl("postgres", "postgres")
    val user = "postgres"
    val password = "postgres"
    val tableName = "FLIGHTS"
    // when
    parquetToPostgre.persistParquetToPostgres(pathToParquet, jdbcUrl, user, password, tableName)
    // then
    try {
      val conn = DriverManager.getConnection(jdbcUrl, user, password)
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery("SELECT COUNT(*) FROM FLIGHTS")
      resultSet.next()
      val count = resultSet.getInt(1)
      assertThat(count).isEqualTo(10_00_000)
    } catch (e: SQLException) {
      e.printStackTrace()
    }
  }
}