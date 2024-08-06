package edu.sample.spark.sql._2_16_getting_started

import org.apache.spark.sql.SparkSession

class CountStudents {
  fun countStudentRecords(): Long {
    val sparkSession =
      SparkSession.builder().appName("CountStudents").master("local[*]").getOrCreate()
    sparkSession.use {
      return sparkSession
        .read()
        .option("header", "true")
        .csv("src/main/resources/exams/students.csv")
        .count()
    }
  }
}
