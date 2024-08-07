package edu.sample.spark.sql._2_17_datasets

import org.apache.spark.sql.SparkSession

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

  fun countRecordsForSubjectAndYearWithJavaFilters(subject: String, year: Int): Long {
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
}
