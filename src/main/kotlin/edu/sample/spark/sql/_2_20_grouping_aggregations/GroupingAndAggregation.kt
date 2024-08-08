package edu.sample.spark.sql._2_20_grouping_aggregations

import org.apache.spark.sql.RowFactory.create
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.Metadata.empty
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class GroupingAndAggregation {
  fun countLogsByLevel(): Map<String, Long> {
    val sparkSession =
      SparkSession.builder().appName("GroupingAndAggregation").master("local[*]").getOrCreate()
    val data =
      listOf(
        create("WARN", "2016-12-31 04:19:32"),
        create("FATAL", "2016-12-31 03:22:34"),
        create("WARN", "2016-12-31 03:21:21"),
        create("INFO", "2015-4-21 14:32:21"),
        create("FATAL", "2015-4-21 19:23:20"),
      )
    val schema =
      StructType(
        arrayOf(
          StructField("level", StringType, false, empty()),
          StructField("datetime", StringType, false, empty()),
        )
      )
    return sparkSession.use {
      it.createDataFrame(data, schema).groupBy("level").count().collectAsList().associate {
        it.getString(0) to it.getLong(1)
      }
    }
  }
}
