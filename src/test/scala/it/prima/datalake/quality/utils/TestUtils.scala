package it.prima.datalake.quality.utils

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TestUtils {

  def getSampleDataFrame(spark: SparkSession): DataFrame = {
    val inputData = Seq(
      Row(1, "A", "AA", 20),
      Row(2, "B", "BB", 30),
      Row(3, "C", "CC", null),
      Row(4, "D", "DD", 50)
    )
    val inputSchema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("surname", StringType, true),
      StructField("age", IntegerType, true)
    ))
    spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      inputSchema
    )
  }
}
