package it.prima.datalake.quality.reader

import org.apache.spark.sql.DataFrame

trait DataReader {

  def read(location: String): DataFrame
}
