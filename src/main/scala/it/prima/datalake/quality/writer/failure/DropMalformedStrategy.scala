package it.prima.datalake.quality.writer.failure
import it.prima.datalake.quality.model.QualityOutput
import org.apache.spark.sql.DataFrame

class DropMalformedStrategy extends FailureHandlerStrategy {
  override def handle(dataFrame: DataFrame): QualityOutput = {
    val performingData = dataFrame//.filter()  ho bisogno del dataframe sorgente per prendermi le righe su cui la condizione non è verificata
    val nonPerformingData = dataFrame//!.filter()
    QualityOutput(performingData, nonPerformingData)
  }
}
