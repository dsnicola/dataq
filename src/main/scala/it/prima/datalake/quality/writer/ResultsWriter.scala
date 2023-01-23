package it.prima.datalake.quality.writer

import it.prima.datalake.quality.model.{QualityResult, Sink}
import it.prima.datalake.quality.writer.failure.FailureHandlerStrategy
import org.apache.spark.sql.DataFrame

abstract class ResultsWriter(failureHandler: FailureHandlerStrategy, sink: Sink) {

  def writeAll(qualityResult: QualityResult): Unit = {
    writeChecks(qualityResult.getCheckResult())
    writeAnalysis(qualityResult.getAnalyzeResult().toDF())
  }

  def writeChecks(dataFrame: DataFrame): Unit = {
    val qualityOutput = failureHandler.handle(dataFrame)
    write(qualityOutput.checkResults, sink.checkPath)
    write(qualityOutput.nonPerformingData, sink.corruptedPath)
  }

  def writeAnalysis(dataFrame: DataFrame): Unit = {
    write(dataFrame, sink.analysisPath)
  }

  protected def write(dataFrame:DataFrame, location: String): Unit

}
