# Learning spark

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
     ```kotlin
     javaSparkContext.parallelize(listOf(1, 2, 3, 4, 5))
     ```
   2. Referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
      ```kotlin
      javaSparkContext.textFile("hdfs://...")
      ```
- There are two kinds of operation that you do with RDD
  1. [Transformation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations): which create a new dataset from an existing one
  2. [Actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions): which return a value to the driver program after running a computation on the dataset
- PairRDD: RDDs that contain key-value pairs. PairRDDs are commonly used to perform aggregations and other operations that require shuffling the data between nodes. Common operations include grouping or aggregating(reducing) data by a key, or joining two datasets.
- Spark provides two types of shared variables: broadcast variables and accumulators. Broadcast variables are used to save a copy of a large dataset on all worker nodes, while accumulators are used to aggregate information across the worker nodes.
- DAG: Directed Acyclic Graph, which is a representation of the computation that needs to be executed on the cluster. It is a series of transformations and actions that are applied to the RDDs in order to get the final result. You can visualize the DAG using the Spark UI in the url `http://localhost:4040/jobs/`.
- **Shuffle**: A shuffle is a process of redistributing data across partitions. It is a mechanism that Spark uses to reorganize the data so that it can be processed in parallel on different nodes. Shuffling is an expensive operation since it involves disk I/O, data serialization, and network I/O. It is important to minimize the number of shuffles in your Spark application to improve performance. A shuffle occurs during a wide transformation, such as `reduceByKey` or `join`. If there is no way to get off from using a shuffle then try to keep them at the end. You can try to chain the transformations to avoid shuffles. or 
- Key skews: When the data is not evenly distributed across the partitions, it is called key skew. Key skew can cause performance issues in your Spark application, as some partitions may have more data to process than others. To avoid key skew, you can try to repartition the data using `repartition` or `coalesce` transformations, or use a custom partitioner to evenly distribute the data across partitions. You can also trying doing "**salting**" to the keys to distribute the data evenly across the partitions which is more of a hack. You can try to use a small data set so that you dont run into OOM when you perform a `groupByKey` .
- `reduceByKey` is better than `groupByKey` because it does the aggregation on the existing partition before it shuffles and then again aggregates for the final output. This is called **Map Side Reduce**. `groupByKey` shuffles all the data to the same key and then does the aggregation which is called **Reduce Side Reduce**.
- Caching is a way to store intermediate results in memory so that they can be reused in subsequent computations. You can cache an RDD using the `cache` method or the `persist` method. Caching is useful when you have an RDD that you want to reuse in multiple actions, or when you want to avoid recomputing the RDD from scratch. You can also specify the storage level when caching with `persist(storageLevel)`, such as `MEMORY_ONLY`, `MEMORY_AND_DISK`, or `MEMORY_ONLY_SER`. You can also specify the replication factor when caching an RDD, such as `MEMORY_ONLY_2`, to store the data on two nodes for fault tolerance. If you wish to clear the cache you can use `unpersist` method which uses LRU i.e. least recently used algorithm to remove the RDD from the cache.
- Spark provides two types of shared variables: broadcast variables and accumulators. Broadcast variables are used to save a copy of a large dataset on all worker nodes, while accumulators are used to aggregate information across the worker nodes.

### RDD Transformations

All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently.

There are two types of transformations: narrow transformations and wide transformations. 
- **Narrow transformations** are those where each input partition will contribute to only one output partition. Examples of narrow transformations include `map`, `mapToPair`, `filter`, and `union`.
- **Wide transformations** are those where each input partition may contribute to many output partitions. Examples of wide transformations include `reduceByKey` and `join`.

#### `map(func)`

is a transformation that passes each element of the RDD through a function and returns a new RDD.

![Map](docs/img/map.svg)

> Example: [MappingTest.kt](src/test/kotlin/edu/sample/spark/core/map/MappingTest.kt)

#### `filter(func)`

is a transformation that returns a new RDD containing only the elements that satisfy a predicate.

![Filter](docs/img/filter.svg)

#### `reduceByKey(func)`

When called on a dataset of `(K, V)` pairs, returns a dataset of `(K, V)` pairs where the values for each key are aggregated using the given reduce function func, which must be of type `(V,V) => V`

![ReduceByKey](docs/img/reduceByKey.svg)

> Example: [ReduceByKeyTest.kt](src/test/kotlin/edu/sample/spark/core/reduce/ReduceByKeyTest.kt)

#### `groupByKey()`

When called on a dataset of `(K, V)` pairs, returns a dataset of `(K, Iterable<V>)` pairs. It is an expensive operation since it shuffles the data.

![GroupByKey](docs/img/groupByKey.svg)

#### `flatMap(func)`

Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).

![FlatMap](docs/img/flatMap.svg)

> Example: [FlatMapTest.kt](src/test/kotlin/edu/sample/spark/core/map/FlatMapTest.kt)

#### `sortByKey()`

When called on a dataset of `(K, V)` pairs where K implements Ordered, returns a dataset of `(K, V)` pairs sorted by keys in ascending or descending order, as specified in the boolean ascending argument. Remember when this is used with `foreach` it will print the output in the order of the key.

#### `join()`

When called on datasets of type `(K, V)` and `(K, W)`, returns a dataset of `(K, (V, W))` pairs with all pairs of elements for each key. Outer joins are also supported.

![Join](docs/img/join.svg)

### RDD Actions

Any Spark operation that returns a value to the driver program is an action. Actions force the evaluation of the transformations required for the RDD they were called on, since they need to actually produce output.

#### `collect()`

is an action that returns all elements of the RDD to the driver program in the form of an array.

#### `count()`

is an action that returns the number of elements in the RDD to the driver program.

#### `take(n)`

is an action that returns an array with the first n elements of the dataset.

#### `first()`

is an action that returns the first element of the RDD to the driver program.

#### `reduce(func)` 

is an action that aggregates the elements of the RDD using a function and returns the final result to the driver program. 

![Reduce](docs/img/reduce.svg)

> Example: [ReduceTest.kt](src/test/kotlin/edu/sample/spark/core/reduce/ReduceTest.kt)

#### `foreach(func)`

is an action that applies a function to all elements of the RDD. It has no return value.


## Exercises

### Keyword ranking

The aim of this activity is to rank the keywords based on the number of times they appear in the text. The input is a text file and the output is keywords that are ordered by the number of appearances. We need to discard the common words like `the`, `a`, `an`, `is`, `are`, etc. The input file is [input.txt](src/main/resources/subtitles/input.txt) and the boring words files is [boringwords.txt](src/main/resources/subtitles/boringwords.txt). As a trail 2, you can try to find the top keywords for [input-spring.txt](src/main/resources/subtitles/input-spring.txt) considering the same [boringwords.txt](src/main/resources/subtitles/boringwords.txt) to be discarded during the computation.

> Solution: [KeywordRankingTest.kt](src/test/kotlin/edu/sample/spark/core/keywordranking/KeywordsRankingTest.kt)

### Big data processing for course metrics

