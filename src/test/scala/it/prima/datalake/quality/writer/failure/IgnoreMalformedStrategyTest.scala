package it.prima.datalake.quality.writer.failure

import it.prima.datalake.quality.model.QualityOutput
import it.prima.datalake.quality.utils.TestUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class IgnoreMalformedStrategyTest extends AnyFunSuite {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("Check that data is returned without any change") {
    val failureHandlerStrategy = new IgnoreMalformedStrategy

    val dataFrame = TestUtils.getSampleDataFrame(spark)
    val actual = failureHandlerStrategy.handle(dataFrame)
    val expected = QualityOutput(dataFrame)

    assert(actual === expected)
  }
}
