package it.prima.datalake.quality.app

import it.prima.datalake.quality.configuration.ConfigReaderFactory
import it.prima.datalake.quality.configuration.parser.ConfigParserFactory
import it.prima.datalake.quality.constants.CSV
import it.prima.datalake.quality.core.abstract_factory.DQFactory
import it.prima.datalake.quality.model.Sink
import it.prima.datalake.quality.reader.{DataReader, DataReaderFactory}
import it.prima.datalake.quality.service.QSStrategyFactory
import it.prima.datalake.quality.utils.SparkContext
import it.prima.datalake.quality.writer.ResultsWriterFactory
import it.prima.datalake.quality.writer.failure.exceptions.FailureStrategyFactory
import org.apache.spark.sql.SparkSession
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}

import scala.collection.JavaConverters._

object DQApp extends SparkContext {

  @Option(name = "--dqEngine", aliases = Array("-e"), usage = "Data quality tool to run with", required = false)
  var dqEngine: String = "DeequEngine"

  @Option(name = "--configurationPath", aliases = Array("-c"), usage = "path where to find the configuration file", required = true)
  var configurationPath: String = _

  @Option(name = "--dataframeLocation", aliases = Array("-d"), usage = "location from where to read delta table", required = true)
  var dataframeLocation: String = _

  @Option(name = "--checkLocation", aliases = Array("-C"), usage = "location where to write the output of data quality checks", required = true)
  var checkLocation: String = _

  @Option(name = "--corruptedLocation", aliases = Array("-X"), usage = "location where to write bad data in case of data quality failures", required = false)
  var corruptedLocation: String = _

  @Option(name = "--analysisLocation", aliases = Array("-A"), usage = "location where to write the output of data analysis", required = false)
  var analysisLocation: String = _

  @Option(name = "--failureStrategy", aliases = Array("-f"), usage = "Strategy to use when bad data has been found by the checks. Default value IGNORE_MALFORMED. Valid values (DROP_MALFORMED, FAIL_FAST)", required = true)
  var failureStrategy: String = "IGNORE_MALFORMED"

  def main(args: Array[String])(implicit spark: SparkSession): Unit = {
    parseArguments(args)
    val reader: DataReader = DataReaderFactory.createDataReader()
    val target = reader.read(dataframeLocation)

    val configParser = ConfigParserFactory.createConfigParser(configurationPath)
    val configReader = ConfigReaderFactory.createConfigReader(configurationPath, configParser)
    val dqConfig = configReader.readConfig()

    val dqFactory = DQFactory.createFactory(dqEngine)
    val qualityServiceStrategy = QSStrategyFactory.createStrategy(dqConfig, dqFactory)
    val qualityResult = qualityServiceStrategy.runQuality(target, dqConfig.groups.orNull)

    val failureStrategyHandler = FailureStrategyFactory.createFailureStrategy(failureStrategy, target)
    val sink = Sink(checkLocation, corruptedLocation, analysisLocation, CSV)
    val resultsWriter = ResultsWriterFactory.createResultsWriter(failureStrategyHandler, sink)
    resultsWriter.writeAll(qualityResult)

    val checkResult = qualityResult.getCheckResult()
    if(checkResult != null) checkResult.show(false)
    val analyzeResult = qualityResult.getAnalyzeResult()
    if(analyzeResult != null) analyzeResult.show(false)
  }

  def parseArguments(args: Array[String]): Unit = {
    val parser = new CmdLineParser(this)
    try {
      parser.parseArgument(args.toList.asJava)
    } catch {
      case e: CmdLineException =>
        parser.printUsage(System.err);
    }
  }

  /**
   * Define di app name spark
   *
   * @return
   */
  override def getAppName: String = "DataQuality"
}
