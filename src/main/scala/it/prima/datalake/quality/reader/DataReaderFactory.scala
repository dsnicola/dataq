package it.prima.datalake.quality.reader

import org.apache.spark.sql.SparkSession

object DataReaderFactory {

  def createDataReader()(implicit spark: SparkSession): DataReader = {
    new DataReaderImpl
  }
}
