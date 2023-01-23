package it.prima.datalake.quality.model

import org.apache.spark.sql.DataFrame

case class QualityOutput(
                          checkResults: DataFrame,
                          nonPerformingData: DataFrame = null
                        )
