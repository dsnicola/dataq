package it.prima.datalake.quality.model

import org.apache.spark.sql.DataFrame

case class QualityOutput(
                        performingData: DataFrame,
                        nonPerformingData: DataFrame = null
                        )
