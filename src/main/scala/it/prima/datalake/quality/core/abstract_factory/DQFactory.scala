package it.prima.datalake.quality.core.abstract_factory

import it.prima.datalake.quality.core.constants.DQEngine
import it.prima.datalake.quality.core.constants.DQEngine.DeequEngine

object DQFactory {

  def createFactory(dqEngine: String): DQAbstractFactory = {
    DQEngine.withName(dqEngine) match {
      case DeequEngine => new DeequFactory
    }
  }
}
