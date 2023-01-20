package it.prima.datalake.quality.core.check

import it.prima.datalake.quality.model.DQConstraint

trait CheckGroup {

  def addExpectation(dqConstraint: DQConstraint): CheckGroup

  def addExpectations(dqConstraints: List[DQConstraint]): CheckGroup

}
