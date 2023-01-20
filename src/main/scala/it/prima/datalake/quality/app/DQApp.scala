package it.prima.datalake.quality.app

import it.prima.datalake.quality.configuration.ConfigReaderFactory
import it.prima.datalake.quality.configuration.parser.ConfigParserFactory
import it.prima.datalake.quality.core.abstract_factory.DQFactory
import it.prima.datalake.quality.service.QSStrategyFactory
import it.prima.datalake.quality.utils.SparkContext
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

  def main(args: Array[String])(implicit spark: SparkSession): Unit = {
    parseArguments(args)
    val target = spark.read.format("delta").load(dataframeLocation)

    val configParser = ConfigParserFactory.createConfigParser(configurationPath)
    val configReader = ConfigReaderFactory.createConfigReader(configurationPath, configParser)
    val dqConfig = configReader.readConfig()

    val dqFactory = DQFactory.createFactory(dqEngine)
    val qualityServiceStrategy = QSStrategyFactory.createStrategy(dqConfig, dqFactory)
    val qualityResult = qualityServiceStrategy.runQuality(target, dqConfig.groups.orNull)

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
