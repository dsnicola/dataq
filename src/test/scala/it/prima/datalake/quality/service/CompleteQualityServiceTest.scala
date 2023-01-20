package it.prima.datalake.quality.service

import it.prima.datalake.quality.core.abstract_factory.DQAbstractFactory
import it.prima.datalake.quality.core.analyzer.Analyzer
import it.prima.datalake.quality.core.suite.Suite
import it.prima.datalake.quality.model.{DQGroup, QualityResult, Suggestion}
import it.prima.datalake.quality.utils.TestUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class CompleteQualityServiceTest extends AnyFunSuite with MockFactory {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("both analyze and check must run") {
    val target = TestUtils.getSampleDataFrame(spark)
    val analyzer = mock[Analyzer]
    (analyzer.analyze _).expects(target).returns(null)

    val suite = mock[Suite]
    (suite.runSuite _).expects().returns(null)
    val dqFactory = mock[DQAbstractFactory]
    (dqFactory.createSuite (_: DataFrame)(_: SparkSession)).expects(target, spark).returns(suite)
    (dqFactory.createAnalyzer (_: SparkSession)).expects(spark).returns(analyzer)

    val strategy = new CompleteQualityService(dqFactory)

    val actual = strategy.runQuality(target, List[DQGroup]())
    val expected = QualityResult(Some(null), Some(null))

    assert(actual === expected)
  }
}
