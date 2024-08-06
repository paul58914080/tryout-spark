package edu.sample.spark.sql._2_17_datasets

import org.apache.spark.sql.SparkSession

class Filter {
  fun firstRecordSubject(sparkSession: SparkSession): String {
    val students =
      sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
    return students.first().getString(2)
  }

  fun firstRecordYear(sparkSession: SparkSession): Int {
    val students =
      sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
    return students.first().getAs<String?>(3).toInt()
  }
}
