package it.prima.datalake.quality.configuration

import it.prima.datalake.quality.configuration.parser.ConfigParser
import it.prima.datalake.quality.model.DQConfig

//TODO
class S3ConfigReader(path: String, configParser: ConfigParser) extends ConfigReader {

  def readConfig(): DQConfig = ???

}
