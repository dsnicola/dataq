package it.prima.datalake.quality.writer.failure.exceptions

import it.prima.datalake.quality.constants.{FailFastStrategy, IgnoreMalformedStrategy}
import it.prima.datalake.quality.writer.failure.{FailFastStrategy, FailureHandlerStrategy, IgnoreMalformedStrategy}

object FailureStrategyFactory {

  def createFailureStrategy(inputStrategy: String): FailureHandlerStrategy = {
    inputStrategy match {
      case IgnoreMalformedStrategy.name => new IgnoreMalformedStrategy
      case FailFastStrategy.name => new FailFastStrategy
      case _ => throw new IllegalArgumentException(s"Non supported strategy $inputStrategy")
    }
  }
}
