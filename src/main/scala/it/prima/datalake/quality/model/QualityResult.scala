package it.prima.datalake.quality.model

import org.apache.spark.sql.{DataFrame, Dataset}

case class QualityResult(checkResult: Option[DataFrame] = None, analyzeResult: Option[Dataset[Suggestion]] = None) {

  def getCheckResult: DataFrame = {
    checkResult match {
      case Some(d) => d
      case None => println("No check has been performed"); null
    }
  }

  def getAnalyzeResult: DataFrame = {
    analyzeResult match {
      case Some(m) => m.toDF()
      case None => println("No analysis has been performed"); null
    }
  }
}
