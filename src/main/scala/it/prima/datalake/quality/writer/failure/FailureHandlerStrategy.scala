package it.prima.datalake.quality.writer.failure

import it.prima.datalake.quality.model.QualityOutput
import org.apache.spark.sql.DataFrame

trait FailureHandlerStrategy {

  def handle(checkResults: DataFrame): QualityOutput
}
