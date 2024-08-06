package edu.sample.spark.util

class WaitForDAG {
  fun pause() {
    // This code snippet is used to keep the Spark UI open
    // http://localhost:4040/jobs/
    val scanner = java.util.Scanner(System.`in`)
    scanner.nextLine()
  }
}
