package it.prima.datalake.quality.app

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class DQAppTest extends AnyFunSuite with BeforeAndAfter {

  final val RESOURCES_PATH = "./src/test/resources"
  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  after {
    FileUtils.deleteDirectory(new File(s"$RESOURCES_PATH/output/actual/checks"))
    FileUtils.deleteDirectory(new File(s"$RESOURCES_PATH/output/actual/analysis"))
  }

  test("system test checks") {
    val deltaLocation = s"$RESOURCES_PATH/sample"
    val configLocation = s"$RESOURCES_PATH/sample_check_config.yaml"
    val checkLocation = s"$RESOURCES_PATH/output/actual/checks"
    val args = Array(
      "--configurationPath", configLocation,
      "--dataframeLocation", deltaLocation,
      "--checkLocation", checkLocation
    )

    DQApp.main(args)

    val actual = spark.read.option("header", "true").csv(checkLocation)
    val expectedLocation = s"$RESOURCES_PATH/output/expected/checks.csv"
    val expected = spark.read.option("header", "true").csv(expectedLocation)

    assert(actual.collectAsList() === expected.collectAsList())
  }

  test("system test analyze") {
    val deltaLocation = s"$RESOURCES_PATH/sample"
    val configLocation = s"$RESOURCES_PATH/analyze_config.yaml"
    val analysisLocation = s"$RESOURCES_PATH/output/actual/analysis"
    val args = Array(
      "--configurationPath", configLocation,
      "--dataframeLocation", deltaLocation,
      "--analysisLocation", analysisLocation
    )

    DQApp.main(args)

    val actual = spark.read.option("header", "true").csv(analysisLocation)
    val expectedLocation = s"$RESOURCES_PATH/output/expected/analysis.csv"
    val expected = spark.read.option("header", "true").csv(expectedLocation)

    actual.show(false)

    assert(actual.collectAsList() === expected.collectAsList())
  }

  test("system test complete") {
    val deltaLocation = s"$RESOURCES_PATH/sample"
    val configLocation = s"$RESOURCES_PATH/sample_complete_config.yaml"
    val checkLocation = s"$RESOURCES_PATH/output/actual/checks"
    val analysisLocation = s"$RESOURCES_PATH/output/actual/analysis"
    val args = Array(
      "--configurationPath", configLocation,
      "--dataframeLocation", deltaLocation,
      "--checkLocation", checkLocation,
      "--analysisLocation", analysisLocation

    )

    DQApp.main(args)

    val actualAnalysis = spark.read.option("header", "true").csv(analysisLocation)
    val expectedAnalysisLocation = s"$RESOURCES_PATH/output/expected/analysis.csv"
    val expectedAnalysis = spark.read.option("header", "true").csv(expectedAnalysisLocation)
    val actualChecks = spark.read.option("header", "true").csv(checkLocation)
    val expectedChecksLocation = s"$RESOURCES_PATH/output/expected/checks.csv"
    val expectedChecks = spark.read.option("header", "true").csv(expectedChecksLocation)

    assert(actualAnalysis.collectAsList() === expectedAnalysis.collectAsList())
    assert(actualChecks.collectAsList() === expectedChecks.collectAsList())
  }

}
