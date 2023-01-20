package it.prima.datalake.quality.writer

import it.prima.datalake.quality.model.{QualityResult, Sink}
import it.prima.datalake.quality.writer.failure.FailureHandlerStrategy
import org.apache.spark.sql.DataFrame

abstract class ResultsWriter(failureHandler: FailureHandlerStrategy, checkSink: Sink, analysisSink: Sink) {

  def writeAll(qualityResult: QualityResult): Unit = {
    writeChecks(qualityResult.getCheckResult())
    writeAnalysis(qualityResult.getAnalyzeResult().toDF())
  }

  def writeChecks(dataFrame: DataFrame): Unit = {
    val qualityOutput = failureHandler.handle(dataFrame)
    write(qualityOutput.performingData, checkSink.path)
    write(qualityOutput.nonPerformingData, checkSink.corruptedPath)
  }

  def writeAnalysis(dataFrame: DataFrame): Unit = {
    write(dataFrame, analysisSink.path)
  }

  protected def write(dataFrame:DataFrame, location: String): Unit

}
