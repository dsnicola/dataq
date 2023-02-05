package it.prima.datalake.quality.writer.failure

import org.apache.spark.sql.DataFrame

trait FailureHandlerStrategy {

  def handle(checkResults: DataFrame): DataFrame
}
