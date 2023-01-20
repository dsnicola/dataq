package it.prima.datalake.quality.core.abstract_factory
import it.prima.datalake.quality.core.analyzer.{Analyzer, DeequAnalyzer}
import it.prima.datalake.quality.core.check.{CheckGroup, DeequCheckGroup}
import it.prima.datalake.quality.core.suite.{DeequSuite, Suite}
import it.prima.datalake.quality.model.DQGroup
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeequFactory extends DQAbstractFactory {

  override def createCheckGroup(dqGroup: DQGroup): CheckGroup = new DeequCheckGroup(dqGroup).addExpectations(dqGroup.checks)

  override def createSuite(dataFrame: DataFrame)(implicit spark: SparkSession) : Suite = new DeequSuite(dataFrame)

  override def createAnalyzer(implicit spark: SparkSession) : Analyzer = new DeequAnalyzer
}
