package it.prima.datalake.quality.core.analyzer

import com.amazon.deequ.suggestions.{ConstraintSuggestionResult, ConstraintSuggestionRunner, Rules}
import it.prima.datalake.quality.model.Suggestion
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class DeequAnalyzer(implicit spark: SparkSession) extends Analyzer {

  import spark.implicits._

  override def analyze(target: DataFrame): Dataset[Suggestion] = {
    val suggestionResult = ConstraintSuggestionRunner()
      .onData(target)
      .addConstraintRules(Rules.DEFAULT)
      .run()
    extractSuggestions(suggestionResult).toDS()
  }

  private def extractSuggestions(suggestionResult: ConstraintSuggestionResult): Seq[Suggestion] = {
    suggestionResult.constraintSuggestions.values
      .foldLeft(Seq[Suggestion]())(_ ++ _.map(s => Suggestion(s.columnName, s.description, s.codeForConstraint)))
  }

  /**
   * private def extractSuggestions(suggestionResult: ConstraintSuggestionResult): Seq[Suggestion] = {
   *  val analysisResult = Seq[Suggestion]()
   *  suggestionResult.constraintSuggestions.foreach {
   *  case (_, suggestions) => suggestions.foreach {
   *  s => analysisResult.update(s.columnName, Suggestion(s.columnName, s.description, s.codeForConstraint))
   *  }
   *  }
   *  analysisResult
   *  }
   */
}
