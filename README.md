# Leaning spark

## Aim

The aim of this project is to learn spark and its ecosystem. I was trying to understand this with the help of a tutorial in udemy [Apache Spark for Java Developers](https://www.udemy.com/course/apache-spark-for-java-developers/?couponCode=24T7MT72224) and while I was learning from the videos I wanted to have a playground to try this out. This project is the playground for me to try out the code and understand the concepts.

## Pre-requisites

- OpenJDK 17
- Maven 3.8.3

## How to build?

```shell
mvn clean install
```

## Learning notes

- Acronym for `RDD` is **Resilient Distributed Dataset**, which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel
- There are two ways to create RDDs: 
  1. Parallelizing an existing collection in your driver program, 
  2. Referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
- There are two kinds of operation that you do with RDD
  1. [Transformation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations): which create a new dataset from an existing one
  2. [Actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions): which return a value to the driver program after running a computation on the dataset
- PairRDD: RDDs that contain key-value pairs. PairRDDs are commonly used to perform aggregations and other operations that require shuffling the data between nodes. Common operations include grouping or aggregating(reducing) data by a key, or joining two datasets.

### RDD Transformations

All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently.

#### Map

##### `map(func)`

is a transformation that passes each element of the RDD through a function and returns a new RDD.

![Map](docs/img/map.svg)

> Example: [MappingTest.kt](src/test/kotlin/edu/sample/spark/core/map/MappingTest.kt)

##### `reduceByKey(func)`

When called on a dataset of `(K, V)` pairs, returns a dataset of `(K, V)` pairs where the values for each key are aggregated using the given reduce function func, which must be of type `(V,V) => V`

![ReduceByKey](docs/img/reduceByKey.svg)

> Example: [ReduceByKeyTest.kt](src/test/kotlin/edu/sample/spark/core/reduce/ReduceByKeyTest.kt)

##### `groupByKey(func)`

When called on a dataset of `(K, V)` pairs, returns a dataset of `(K, Iterable<V>)` pairs. It is an expensive operation since it shuffles the data.

![GroupByKey](docs/img/groupByKey.svg)

##### `flatMap(func)`

Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).

> Example: [FlatMapTest.kt](src/test/kotlin/edu/sample/spark/core/map/FlatMapTest.kt)

### RDD Actions

Any Spark operation that returns a value to the driver program is an action. Actions force the evaluation of the transformations required for the RDD they were called on, since they need to actually produce output.

#### Reduce

##### `reduce(func)` 

is an action that aggregates the elements of the RDD using a function and returns the final result to the driver program. 

![Reduce](docs/img/reduce.svg)

> Example: [ReduceTest.kt](src/test/kotlin/edu/sample/spark/core/reduce/ReduceTest.kt)

##### `foreach(func)`

is an action that applies a function to all elements of the RDD. It has no return value.


## Exercises

### Keyword ranking

The aim of this activity is to rank the keywords based on the number of times they appear in the text. The input is a text file and the output is keywords that are ordered by the number of appearances. We need to discard the common words like `the`, `a`, `an`, `is`, `are`, etc. The input file is [input.txt](src/main/resources/subtitles/input.txt) and the boring words files is [boringwords.txt](src/main/resources/subtitles/boringwords.txt). As a trail 2, you can try to find the top keywords for [input-spring.txt](src/main/resources/subtitles/input-spring.txt) considering the same [boringwords.txt](src/main/resources/subtitles/boringwords.txt) to be discarded during the computation.

> Solution: [KeywordRankingTest.kt](src/test/kotlin/edu/sample/spark/core/keywordranking/KeywordsRankingTest.kt)

### Big data processing for course metrics

