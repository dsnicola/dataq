package it.prima.datalake.quality.utils

import org.apache.spark.sql.SparkSession

/**
 * Trait to create a spark session with preconfigured config
 */
trait SparkContext {

  implicit lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName(getAppName)
      .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.databricks.delta.optimizeWrite.enabled", "true")
      .config("spark.databricks.delta.autoCompact.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * Define di app name spark
   *
   * @return
   */
  def getAppName: String

}
