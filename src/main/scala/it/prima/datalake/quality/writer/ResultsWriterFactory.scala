package it.prima.datalake.quality.writer

import it.prima.datalake.quality.model.Sink
import it.prima.datalake.quality.writer.failure.FailureHandlerStrategy

object ResultsWriterFactory {

  def createResultsWriter(failureHandler: FailureHandlerStrategy, checkSink: Sink): ResultsWriter = {
    new ResultsWriterImpl(failureHandler, checkSink)
  }
}
