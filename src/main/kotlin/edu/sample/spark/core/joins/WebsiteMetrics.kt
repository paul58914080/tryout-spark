package edu.sample.spark.core.joins

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.Optional
import scala.Tuple2

class WebsiteMetrics(
  val visitsStore: List<Tuple2<Long, Long>>,
  val usersStore: List<Tuple2<Long, String>>,
) {
  val configuration: SparkConf = SparkConf().setAppName("WebsiteMetrics").setMaster("local[*]")
  val sc = JavaSparkContext(configuration)

  fun getMetricOnlyForAvailableUserAndVisit(): List<Tuple2<Long, Tuple2<Long, String>>> {
    return sc.use {
      val visitsPairRDD = sc.parallelizePairs(visitsStore)
      val userPairRDD = sc.parallelizePairs(usersStore)
      visitsPairRDD.join(userPairRDD).collect()
    }
  }

  fun getMetricForAllUsersVisitsButNotAvailableInUserStore(): List<Tuple2<Long, Tuple2<Long, Optional<String>>>> {
    return sc.use {
      val visitsPairRDD = sc.parallelizePairs(visitsStore)
      val userPairRDD = sc.parallelizePairs(usersStore)
      visitsPairRDD.leftOuterJoin(userPairRDD).collect()
    }
  }



  fun getMetricForAllUsersVisitsButNotAvailableInVisitStore(): List<Tuple2<Long, Tuple2<Optional<Long>, String>>> {
    return sc.use {
      val visitsPairRDD = sc.parallelizePairs(visitsStore)
      val userPairRDD = sc.parallelizePairs(usersStore)
      visitsPairRDD.rightOuterJoin(userPairRDD).collect()
    }
  }
}
