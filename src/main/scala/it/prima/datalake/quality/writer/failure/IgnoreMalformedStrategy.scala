package it.prima.datalake.quality.writer.failure
import org.apache.spark.sql.DataFrame

class IgnoreMalformedStrategy extends FailureHandlerStrategy {
  def handle(checkResults: DataFrame): DataFrame = {
    checkResults
  }
}
