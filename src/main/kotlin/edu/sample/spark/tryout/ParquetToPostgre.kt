package edu.sample.spark.tryout

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import java.io.StringReader
import java.sql.Connection
import java.sql.DriverManager

class ParquetToPostgre {

  fun persistParquetToPostgres(pathToParquet: String, jdbcUrl: String, user: String, password: String, tableName: String) {
    val spark = SparkSession.builder()
      .appName("ParquetToPostgres")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "200") // Increase parallelism
      .getOrCreate()

    val parquetFileDF: Dataset<Row> = spark.read().parquet(pathToParquet)
    parquetFileDF.foreachPartition { partition: Iterator<Row?> ->
      var connection: Connection? = null
      try {
        connection = DriverManager.getConnection(jdbcUrl, user, password)
        val copyManager = CopyManager(connection.unwrap(BaseConnection::class.java))

        val copyQuery = "COPY $tableName FROM STDIN WITH (FORMAT csv)"
        val csvData = StringBuilder()

        for (row in partition) {
          // Convert each row to CSV format
          val csvRow = row?.mkString(",")
          csvData.append(csvRow).append("\n")
        }

        val reader = StringReader(csvData.toString())
        copyManager.copyIn(copyQuery, reader)
      } catch (e: Exception) {
        e.printStackTrace()
      } finally {
        connection?.close()
      }
    }

    spark.stop()
  }
}