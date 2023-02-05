package it.prima.datalake.quality.writer

import it.prima.datalake.quality.constants.CSV
import it.prima.datalake.quality.model.{QualityResult, Sink}
import it.prima.datalake.quality.utils.TestUtils
import it.prima.datalake.quality.writer.failure.FailureHandlerStrategy
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class ResultsWriterImplTest extends AnyFunSuite with MockFactory with BeforeAndAfter {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val checkPath = "./src/test/resources/writer/check"
  val analysisPath = "./src/test/resources/writer/analysis"
  val failureHandler: FailureHandlerStrategy = mock[FailureHandlerStrategy]
  val resultsWriter = new ResultsWriterImpl(failureHandler, Sink(checkPath, analysisPath, CSV))

  after {
    FileUtils.deleteDirectory(new File("./src/test/resources/writer"))
  }

  test("write checks") {
    val dataFrame = TestUtils.getSampleDataFrameAllString(spark)
    (failureHandler.handle _).expects(dataFrame).returns(dataFrame)

    resultsWriter.writeChecks(dataFrame)

    val actual = spark.read.option("header", "true").csv(checkPath)

    assert(actual.collectAsList() === dataFrame.collectAsList())
  }

  test("write analysis") {
    val dataFrame = TestUtils.getSampleDataFrameAllString(spark)
    resultsWriter.writeAnalysis(dataFrame)

    val actual = spark.read.option("header", "true").csv(analysisPath)

    assert(actual.collectAsList() === dataFrame.collectAsList())
  }

  test("write all") {
    val dataFrame = TestUtils.getSampleDataFrameAllString(spark)
    val qualityResult = QualityResult(Some(dataFrame))
    (failureHandler.handle _).expects(dataFrame).returns(dataFrame)

    resultsWriter.writeAll(qualityResult)

    val actual = spark.read.option("header", "true").csv(checkPath)

    assert(actual.collectAsList() === dataFrame.collectAsList())
  }
}
