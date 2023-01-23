package it.prima.datalake.quality.writer.failure
import it.prima.datalake.quality.model.QualityOutput
import org.apache.spark.sql.DataFrame

class DropMalformedStrategy(checkData: DataFrame) extends FailureHandlerStrategy {
  override def handle(checkResults: DataFrame): QualityOutput = {
    val nonPerformingData = checkData//!.filter()
    QualityOutput(checkResults, nonPerformingData)
  }
}
