package it.prima.datalake.quality.core.abstract_factory

import it.prima.datalake.quality.core.analyzer.Analyzer
import it.prima.datalake.quality.core.check.CheckGroup
import it.prima.datalake.quality.core.suite.Suite
import it.prima.datalake.quality.model.DQGroup
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DQAbstractFactory {

  def createCheckGroup(dqGroup: DQGroup): CheckGroup

  def createSuite(dataFrame: DataFrame)(implicit spark: SparkSession): Suite

  def createAnalyzer(implicit spark: SparkSession): Analyzer

}
