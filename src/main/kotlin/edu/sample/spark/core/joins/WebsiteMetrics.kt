package edu.sample.spark.core.joins

import java.io.Serializable
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.Optional
import scala.Tuple2

class WebsiteMetrics(val visitsStore: List<Visit>, val usersStore: List<User>) {
  val configuration: SparkConf = SparkConf().setAppName("WebsiteMetrics").setMaster("local[*]")
  val sc = JavaSparkContext(configuration)

  fun getMetricOnlyForAvailableUserAndVisit(): List<Tuple2<Long, Tuple2<Long, String>>> {
    return sc.use {
      val visitsPairRDD = getVisitsAsPairRDD()
      val userPairRDD = getUsersAsPairRDD()
      visitsPairRDD.join(userPairRDD).collect()
    }
  }

  fun getMetricForAllUsersVisitsButNotAvailableInUserStore():
    List<Tuple2<Long, Tuple2<Long, Optional<String>>>> {
    return sc.use {
      val visitsPairRDD = getVisitsAsPairRDD()
      val userPairRDD = getUsersAsPairRDD()
      visitsPairRDD.leftOuterJoin(userPairRDD).collect()
    }
  }

  fun getMetricForAllUsersVisitsButNotAvailableInVisitStore():
    List<Tuple2<Long, Tuple2<Optional<Long>, String>>> {
    return sc.use {
      val visitsPairRDD = getVisitsAsPairRDD()
      val userPairRDD = getUsersAsPairRDD()
      visitsPairRDD.rightOuterJoin(userPairRDD).collect()
    }
  }

  private fun getUsersAsPairRDD(): JavaPairRDD<Long, String> =
    sc.parallelize(usersStore).mapToPair { Tuple2(it.userId, it.username) }

  private fun getVisitsAsPairRDD(): JavaPairRDD<Long, Long> =
    sc.parallelize(visitsStore).mapToPair { Tuple2(it.userId, it.hits) }
}

data class User(val userId: Long, val username: String) : Serializable

data class Visit(val userId: Long, val hits: Long) : Serializable
