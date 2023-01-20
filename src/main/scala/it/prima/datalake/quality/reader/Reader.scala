package it.prima.datalake.quality.reader

import org.apache.spark.sql.DataFrame

trait Reader {

  def read(location: String): DataFrame
}
