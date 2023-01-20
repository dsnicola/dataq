package it.prima.datalake.quality.core.suite

import it.prima.datalake.quality.core.check.CheckGroup
import org.apache.spark.sql.DataFrame

trait Suite {

  def addCheckGroup(checkGroup: CheckGroup): Suite

  def runSuite(): DataFrame

}
