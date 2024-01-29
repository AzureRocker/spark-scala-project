package org.example

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkHelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkHelloWorld")
      .master("local[*]")
      .getOrCreate()

    // Create a DataFrame
    val schema = StructType(List(
      StructField("Name", StringType, true),
      StructField("Age", IntegerType, true)
    ))

    val data = Seq(
      Row("Alice RamBABU", 25),
      Row("Bob Mohan", 30),
      Row("Charlie Krishna", 22)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the DataFrame
    df.show()

    spark.stop()
  }
}
