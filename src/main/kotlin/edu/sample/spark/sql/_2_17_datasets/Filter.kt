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

  fun countRecordsForSubject(sparkSession: SparkSession, subject: String): Long {
    val students =
      sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
    return students.filter("subject = '$subject'").count() // this is like a `WHERE` clause in SQL
  }

  fun countRecordsForSubjectAndYear(sparkSession: SparkSession, subject: String, year: Int): Long {
    val students =
      sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
    return students
      .filter { row ->
        row.getAs<String>("subject") == subject && row.getAs<String>("year").toInt() == year
      }
      .count()
  }

  fun countRecordsBySubjectAndYearWithColFilters(
    sparkSession: SparkSession,
    subject: String,
    year: Int,
  ): Long {
    val students =
      sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
    val subjectCol = students.col("subject")
    val yearCol = students.col("year")
    return students.filter(subjectCol.like(subject).and(yearCol.eqNullSafe(year))).count()
  }
}
