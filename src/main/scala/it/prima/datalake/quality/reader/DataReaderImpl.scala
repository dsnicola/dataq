package it.prima.datalake.quality.reader
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataReaderImpl(implicit spark: SparkSession) extends DataReader {
  override def read(location: String): DataFrame = {
    spark.read.format("delta").load(location)
  }
}
