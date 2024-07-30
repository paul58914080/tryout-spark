package edu.sample.spark.core.keywordranking

import java.io.Serializable
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

class KeywordRanking(val filename: String? = null) : Serializable {
  companion object {
    private const val serialVersionUID = 1L
  }

  val configuration: SparkConf = SparkConf().setAppName("KeywordRanking").setMaster("local[*]")
  val boringwords = getBoringWords()

  fun getTopNKeywords(n: Int): List<String> {
    val sc = JavaSparkContext(configuration)
    return sc.use {
      it
        .textFile("src/main/resources/subtitles/${filename?:"input.txt"}")
        /*
        for each line filter the line which has number i.e. id of the course and the time

        16
        00:00:40,449 --> 00:00:43,668
        -> """^[a-zA-Z${".,"}_ ]*${'$'}""".toRegex() matches it

        remove blank lines
        -> it.trim().isNotEmpty()
        */
        .filter { filterSentences(it) && filterSpaceAndBlankLine(it) }
        .flatMap { it.split(" ").iterator() }
        .map { it.replace(",", "") }
        .filter { word -> !boringwords.contains(word.lowercase()) }
        .mapToPair { word -> Tuple2(word, 1L) }
        .reduceByKey { a, b -> a + b }
        .mapToPair { Tuple2(it._2, it._1) }
        .sortByKey(false)
        .map { it._2 }
        .take(n)
    }
  }

  private fun filterSpaceAndBlankLine(it: String) = it.trim().isNotEmpty()

  private fun filterSentences(it: String) = """^[a-zA-Z${".,"}_ ]*${'$'}""".toRegex() matches it

  fun getBoringWords(): List<String> {
    val sc = JavaSparkContext(configuration)
    return sc.use { it.textFile("src/main/resources/subtitles/boringwords.txt").collect() }
  }
}
