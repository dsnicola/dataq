package it.prima.datalake.quality.reader

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{a, convertToAnyMustWrapper}

class DataReaderFactoryTest extends AnyFunSuite {

  test("DataReaderImpl is returned") {
    val dataReader = DataReaderFactory.createDataReader()(null)

    dataReader mustBe a[DataReaderImpl]
  }

}
