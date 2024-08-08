package edu.sample.spark.sql._2_19_inmemory

import org.apache.spark.sql.RowFactory.create
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata.empty
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class Inmemory {
  fun createDataFrameFromListHaving4ItemsOfLogsAndGetCount(): Long {
    val sparkSession = SparkSession.builder().appName("Inmemory").master("local[*]").getOrCreate()
    return sparkSession.use {
      val logs =
        listOf(
          create("WARN", "2016-12-31 04:19:32", "User 3"),
          create("FATAL", "2016-12-31 03:22:34", "User 4"),
          create("INFO", "2016-12-31 03:21:21", "User 3"),
          create("WARN", "2016-12-31 03:22:21", "User 4"),
          create("FATAL", "2016-12-31 03:23:34", "User 3"),
        )
      val schema =
        StructType(
          arrayOf(
            StructField("level", DataTypes.StringType, false, empty()),
            StructField("datetime", DataTypes.StringType, false, empty()),
            StructField("user", DataTypes.StringType, false, empty()),
          )
        )
      it.createDataFrame(logs, schema).count()
    }
  }
}
