package it.prima.datalake.quality.service

import it.prima.datalake.quality.core.abstract_factory.DQAbstractFactory
import it.prima.datalake.quality.model.{DQGroup, QualityResult, Suggestion}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

abstract class QualityServiceStrategy(dqFactory: DQAbstractFactory)(implicit spark: SparkSession) {

  def runQuality(target: DataFrame, groups: List[DQGroup]): QualityResult

  protected def check(target: DataFrame, groups: List[DQGroup]): DataFrame = {
    val suite = dqFactory.createSuite(target)
    groups.foreach(group => suite.addCheckGroup(dqFactory.createCheckGroup(group)))
    suite.runSuite()
  }

  protected def analyze(target: DataFrame): Dataset[Suggestion] = {
    val analyzer = dqFactory.createAnalyzer
    analyzer.analyze(target)
  }
}
