package it.prima.datalake.quality.configuration

import it.prima.datalake.quality.configuration.parser.ConfigParser

object ConfigReaderFactory {

  private final val S3_PREFIX = "s3a?.*"

  def createConfigReader(configurationPath: String, configParser: ConfigParser): ConfigReader = {
    configurationPath match {
      case p if p.matches(S3_PREFIX) => new S3ConfigReader(p, configParser)
      case default => new LocalConfigReader(default, configParser)
    }
  }

}
