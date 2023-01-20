package it.prima.datalake.quality.core.analyzer

import it.prima.datalake.quality.model.Suggestion
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DeequAnalyzerTest extends AnyFunSuite {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("analyze dataframe") {
    import spark.implicits._

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
    val inputDataframe = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      inputSchema
    )

    val actual = new DeequAnalyzer().analyze(inputDataframe).sort("columnName", "diagnosis")
    val expected = Seq[Suggestion](
      Suggestion("name","'name' is not null",".isComplete(\"name\")"),
      Suggestion("age","'age' has less than 68% missing values",".hasCompleteness(\"age\", _ >= 0.32, Some(\"It should be above 0.32!\"))"),
      Suggestion("age","'age' has no negative values",".isNonNegative(\"age\")"),
      Suggestion("surname","'surname' is not null",".isComplete(\"surname\")"),
      Suggestion("id","'id' has no negative values",".isNonNegative(\"id\")"),
      Suggestion("id","'id' is not null",".isComplete(\"id\")")
    ).toDS().sort("columnName", "diagnosis")

    assert(actual.collectAsList() === expected.collectAsList())
  }
}
