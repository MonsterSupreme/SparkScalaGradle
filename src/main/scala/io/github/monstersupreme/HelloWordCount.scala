package io.github.monstersupreme

import org.apache.spark.sql.SparkSession

object HelloWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().appName("Hello, Word Count!").master("local[*]").getOrCreate()
    println("Spark version: " + spark.version)

    val lines = List("Hello, Word Count!", "Hello, Spark!")
    val rdd = spark.sparkContext.parallelize(lines)
    rdd.flatMap(line => line.split("\\W+"))
      .map(word => (word, 1)).reduceByKey((c1, c2) => c1 + c2)
      .sortByKey().collect()
      .foreach(pair => println(pair._1 + ": " + pair._2))

    spark.stop()
  }
}
