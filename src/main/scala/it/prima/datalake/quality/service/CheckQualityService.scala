package it.prima.datalake.quality.service

import it.prima.datalake.quality.core.abstract_factory.DQAbstractFactory
import it.prima.datalake.quality.model.{DQGroup, QualityResult}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CheckQualityService(dqFactory: DQAbstractFactory)(implicit spark: SparkSession) extends QualityServiceStrategy(dqFactory) {

  override def runQuality(target: DataFrame, groups: List[DQGroup]): QualityResult = {
    QualityResult(checkResult = Some(super.check(target, groups)))
  }

}
