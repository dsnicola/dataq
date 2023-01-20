package it.prima.datalake.quality.core.analyzer

import it.prima.datalake.quality.model.Suggestion
import org.apache.spark.sql.{DataFrame, Dataset}

trait Analyzer {

  def analyze(target: DataFrame): Dataset[Suggestion]

}
