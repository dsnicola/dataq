package it.prima.datalake.quality.writer.failure

import it.prima.datalake.quality.model.QualityOutput
import it.prima.datalake.quality.writer.failure.exceptions.BadDatasetError
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class FailFastStrategy extends FailureHandlerStrategy {

  private final val CHECK_STATUS = "check_status"

  override def handle(checkResults: DataFrame): QualityOutput = {
    val errors = checkResults.filter(col(CHECK_STATUS) === lit("Error"))
    if(!errors.isEmpty) throw new BadDatasetError("Data quality result has constraint violation. Exiting because of the FAIL_FAST strategy.")
    QualityOutput(checkResults)
  }
}
