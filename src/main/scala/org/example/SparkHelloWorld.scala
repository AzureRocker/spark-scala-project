package org.example

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkHelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Test Mavon Hello")
      .master("local[*]")
      .getOrCreate()

    // Create a DataFrame
    val schema = StructType(List(
      StructField("Name", StringType, true),
      StructField("Age", IntegerType, true)
    ))

    val data = Seq(
      Row("Alice RamBABU", 45),
      Row("Bob Mohan", 54),
      Row("Charlie Krishna", 32)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the DataFrame
    df.show()

    spark.stop()
  }
}
