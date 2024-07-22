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

- Acronym for RDD is **Resilient Distributed Dataset**
- `reduce()` is an action that aggregates the elements of the RDD using a function and returns the final result to the driver program. [ReduceTest.kt](src/test/kotlin/edu/kotlin/spark/reduce/ReduceTest.kt)
- `map()` is a transformation that passes each element of the RDD through a function and returns a new RDD. [MappingTest.kt](src/test/kotlin/edu/kotlin/spark/map/MappingTest.kt)