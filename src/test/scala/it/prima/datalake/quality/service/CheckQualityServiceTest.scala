package it.prima.datalake.quality.service

import it.prima.datalake.quality.core.abstract_factory.DQAbstractFactory
import it.prima.datalake.quality.core.suite.Suite
import it.prima.datalake.quality.model.{DQGroup, QualityResult}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class CheckQualityServiceTest extends AnyFunSuite with MockFactory {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("only check is performed") {
    val suite = mock[Suite]
    val dqFactory = mock[DQAbstractFactory]
    (suite.runSuite _).expects().returns(null)
    (dqFactory.createSuite (_: DataFrame)(_: SparkSession)).expects(null, spark).returns(suite)

    val strategy = new CheckQualityService(dqFactory)

    val actual = strategy.runQuality(null, List[DQGroup]())
    val expected = QualityResult(checkResult = Some(null))

    assert(actual === expected)
  }
}
