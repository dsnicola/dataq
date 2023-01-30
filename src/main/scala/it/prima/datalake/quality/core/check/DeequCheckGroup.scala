package it.prima.datalake.quality.core.check

import com.amazon.deequ.checks.{Check, CheckLevel}
import it.prima.datalake.quality.model.{DQConstraint, DQGroup}

class DeequCheckGroup(dqGroup: DQGroup) extends CheckGroup {

  private[core] var check: Check = Check(CheckLevel.withName(dqGroup.severity), dqGroup.description)

  override def addExpectation(dqConstraint: DQConstraint): CheckGroup = {
    convertConstraint(dqConstraint)
    this
  }

  override def addExpectations(dqConstraints: List[DQConstraint]): CheckGroup = {
    dqConstraints.foreach(c => addExpectation(c))
    this
  }

  // TODO add all the possible checks with their alternative calls
  def convertConstraint(dqConstraint: DQConstraint): Unit = {
    dqConstraint.check_type match {
      case "isUnique" => dqConstraint.columns.foreach(c => check = check.isUnique(c))
      case "isComplete" => dqConstraint.columns.foreach(c => check = check.isComplete(c))
      case "isNonNegative" => dqConstraint.columns.foreach(c => check = check.isNonNegative(c))
      case "hasCompleteness" => dqConstraint.columns.foreach(c => check = check.hasCompleteness(c, getComparisonFunction(dqConstraint.operator.get, dqConstraint.value.get.toDouble), Some(s"$c should be above ${dqConstraint.value.get.toDouble}")))
      case "isContainedIn" => dqConstraint.columns.foreach(c => check = check.isContainedIn(c, dqConstraint.value_list.get.toArray, Some(s"Following condition should be true: ${dqConstraint.value_list.get.head} <= $c <= ${dqConstraint.value_list.get.last}")))
    }
  }

  def getComparisonFunction(operator: String, value: Double): Double => Boolean = {
    operator match {
      case ">=" => _ >= value
      case "<=" => _ <= value
      case "=" => _ == value
      case "<" => _ < value
      case ">" => _ > value
    }
  }
}
