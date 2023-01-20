package it.prima.datalake.quality.service

import it.prima.datalake.quality.core.abstract_factory.DQAbstractFactory
import it.prima.datalake.quality.core.analyzer.Analyzer
import it.prima.datalake.quality.model.{QualityResult, Suggestion}
import it.prima.datalake.quality.utils.TestUtils
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

class AnalyzeQualityServiceTest extends AnyFunSuite with MockFactory {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("only analysis is performed") {
    val target = TestUtils.getSampleDataFrame(spark)
    val analyzer = mock[Analyzer]
    (analyzer.analyze _).expects(target).returns(null)

    val dqFactory = mock[DQAbstractFactory]
    (dqFactory.createAnalyzer(_: SparkSession)).expects(spark).returns(analyzer)

    val strategy = new AnalyzeQualityService(dqFactory)
    val actual = strategy.runQuality(target, null)
    val expected = QualityResult(analyzeResult = Some(null))
    assert(actual === expected)
  }
}
