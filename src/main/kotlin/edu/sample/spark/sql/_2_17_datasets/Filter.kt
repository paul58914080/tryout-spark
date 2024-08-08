package edu.sample.spark.sql._2_17_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class Filter {

  fun firstRecordSubject(): String {
    val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()
    return sparkSession.use {
      val students =
        sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
      students.first().getString(2)
    }
  }

  fun firstRecordYear(): Int {
    val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()
    return sparkSession.use {
      val students =
        sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
      students.first().getAs<String?>(3).toInt()
    }
  }

  fun countRecordsForSubjectWithSqlFilters(subject: String): Long {
    val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()
    return sparkSession.use {
      val students =
        sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
      students.filter("subject = '$subject'").count() // this is like a `WHERE` clause in SQL
    }
  }

  fun countRecordsForSubjectAndYearWithJavaLambdaFilters(subject: String, year: Int): Long {
    val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()
    return sparkSession.use {
      val students =
        sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
      students
        .filter { row ->
          row.getAs<String>("subject") == subject && row.getAs<String>("year").toInt() == year
        }
        .count()
    }
  }

  fun countRecordsForSubjectAndYearWithColFilters(subject: String, year: Int): Long {
    val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()
    return sparkSession.use {
      val students =
        sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
      val subjectCol = students.col("subject")
      val yearCol = students.col("year")
      students.filter(subjectCol.like(subject).and(yearCol.eqNullSafe(year))).count()
    }
  }

  fun countRecordsForSubjectAndYearWithFunctionColFilters(subject: String, year: Int): Long {
    val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()
    return sparkSession.use {
      val students =
        sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
      students
        // col is a function that returns a Column and its alias is column function
        .filter(col("subject").like(subject).and(col("year").eqNullSafe(year)))
        .count()
    }
  }

  fun maxScoreForSubjectWithTempViewFilters(subject: String): Int {
    val sparkSession = SparkSession.builder().appName("FilterTest").master("local[*]").getOrCreate()
    return sparkSession.use {
      val students =
        sparkSession.read().option("header", "true").csv("src/main/resources/exams/students.csv")
      students.createOrReplaceTempView("students")
      val result =
        sparkSession.sql("SELECT MAX(score) as max_score FROM students WHERE subject = '$subject'")
      result.first().getAs<String>("max_score")?.toInt() ?: 0
    }
  }
}
