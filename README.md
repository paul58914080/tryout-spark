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

### RDD Transformations

All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently.

#### Map

##### `map(func)`

is a transformation that passes each element of the RDD through a function and returns a new RDD. [MappingTest.kt](src/test/kotlin/edu/kotlin/spark/map/MappingTest.kt)

![Map](docs/img/map.svg)

>   Example: [MappingTest.kt](src/test/kotlin/edu/sample/spark/core/map/MappingTest.kt)

### RDD Actions

Any Spark operation that returns a value to the driver program is an action. Actions force the evaluation of the transformations required for the RDD they were called on, since they need to actually produce output.

#### Reduce

##### `reduce(func)` 

is an action that aggregates the elements of the RDD using a function and returns the final result to the driver program. 

![Reduce](docs/img/reduce.svg)

>   Example: [ReduceTest.kt](src/test/kotlin/edu/sample/spark/core/reduce/ReduceTest.kt)





