package it.prima.datalake.quality.writer

import it.prima.datalake.quality.model.Sink
import it.prima.datalake.quality.writer.failure.FailureHandlerStrategy
import org.apache.spark.sql.{DataFrame, SaveMode}

class ResultsWriterImpl(failureHandler: FailureHandlerStrategy, sink: Sink) extends ResultsWriter(failureHandler, sink) {

  override protected def write(dataFrame: DataFrame, location: String): Unit = {
    if (dataFrame != null)
      dataFrame.coalesce(1).write.mode(SaveMode.Overwrite).options(Map("header" -> "true")).csv(location)
  }
}
