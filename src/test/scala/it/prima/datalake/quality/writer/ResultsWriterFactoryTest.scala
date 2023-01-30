package it.prima.datalake.quality.writer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{a, convertToAnyMustWrapper}

class ResultsWriterFactoryTest extends AnyFunSuite {

  test("ResultWriterImpl is correctly created") {
    val resultsWriter = ResultsWriterFactory.createResultsWriter(null, null)

    resultsWriter mustBe a[ResultsWriterImpl]
  }
}
