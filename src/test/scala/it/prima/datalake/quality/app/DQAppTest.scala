package it.prima.datalake.quality.app

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class DQAppTest extends AnyFunSuite {

  final val RESOURCES_PATH = "./src/test/resources"
  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("system test") {
    val deltaLocation = s"$RESOURCES_PATH/gold_table"
    val configLocation = s"$RESOURCES_PATH/config.yaml"
    val args = Array(
      "--configurationPath", configLocation,
      "--dataframeLocation", deltaLocation
    )

    DQApp.main(args)
  }

  test("system test analyze") {
    val deltaLocation = s"$RESOURCES_PATH/gold_table"
    val configLocation = s"$RESOURCES_PATH/analyze_config.yaml"
    val args = Array(
      "--configurationPath", configLocation,
      "--dataframeLocation", deltaLocation
    )

    DQApp.main(args)
  }

  test("system test complete") {
    val deltaLocation = s"$RESOURCES_PATH/gold_table"
    val configLocation = s"$RESOURCES_PATH/complete_config.yaml"
    val args = Array(
      "--configurationPath", configLocation,
      "--dataframeLocation", deltaLocation
    )

    DQApp.main(args)
  }

}
