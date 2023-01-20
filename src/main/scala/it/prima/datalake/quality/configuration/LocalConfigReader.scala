package it.prima.datalake.quality.configuration

import it.prima.datalake.quality.configuration.parser.ConfigParser
import it.prima.datalake.quality.model.DQConfig
import org.apache.commons.io.FileUtils

import java.io.File

class LocalConfigReader(path: String, configParser: ConfigParser) extends ConfigReader {

  def readConfig(): DQConfig = {
    val fileContent = FileUtils.readFileToString(new File(path), "UTF-8")
    configParser.parse(fileContent)
  }

}
