package it.prima.datalake.quality.writer.failure

import it.prima.datalake.quality.utils.TestUtils
import it.prima.datalake.quality.writer.failure.exceptions.BadDatasetError
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class FailFastStrategyTest extends AnyFunSuite {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("check error is thrown") {
    val failedChecks = TestUtils.getFailedChecks(spark)

    assertThrows[BadDatasetError] {
      new FailFastStrategy().handle(failedChecks)
    }
  }
  test("check results are correctly returned if they don't contain errors") {
    val checks = TestUtils.getSuccessChecks(spark)

    val actual = new FailFastStrategy().handle(checks)
    val expected = checks

    assert(actual.collectAsList() === expected.collectAsList())
  }
}
