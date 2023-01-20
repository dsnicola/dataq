package it.prima.datalake.quality.service

import it.prima.datalake.quality.core.abstract_factory.DQAbstractFactory
import it.prima.datalake.quality.model.{DQConfig, DQGroup}
import org.apache.spark.sql.SparkSession

object QSStrategyFactory {

  def createStrategy(dqConfig: DQConfig, dqFactory: DQAbstractFactory)(implicit spark: SparkSession): QualityServiceStrategy = {
    dqConfig.groups match {
      case Some(g) => if (dqConfig.analyze.getOrElse(false)) new CompleteQualityService(dqFactory) else new CheckQualityService(dqFactory)
      case None => new AnalyzeQualityService(dqFactory)
    }
  }

}
