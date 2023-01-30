package it.prima.datalake.quality.reader

import it.prima.datalake.quality.utils.TestUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class DataReaderImplTest extends AnyFunSuite {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  test("delta table is returned as dataframe") {
    val dataReader = new DataReaderImpl()

    val location = "./src/test/resources/sample"
    val actual = dataReader.read(location)
    val expected = TestUtils.getSampleDataFrame(spark)

    assert(actual.collectAsList() === expected.collectAsList())
  }
}
