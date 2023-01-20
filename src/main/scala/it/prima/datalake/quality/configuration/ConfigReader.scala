package it.prima.datalake.quality.configuration

import it.prima.datalake.quality.model.DQConfig

trait ConfigReader {

  def readConfig(): DQConfig

}
