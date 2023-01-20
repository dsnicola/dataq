package it.prima.datalake.quality.writer.failure
import it.prima.datalake.quality.model.QualityOutput
import org.apache.spark.sql.DataFrame

class IgnoreMalformedStrategy extends FailureHandlerStrategy {
  override def handle(dataFrame: DataFrame): QualityOutput = {
    QualityOutput(dataFrame)
  }
}
