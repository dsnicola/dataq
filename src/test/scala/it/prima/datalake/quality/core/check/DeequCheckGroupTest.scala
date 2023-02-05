package it.prima.datalake.quality.core.check

import com.amazon.deequ.checks.{Check, CheckLevel}
import it.prima.datalake.quality.model.{DQConstraint, DQGroup}
import org.scalatest.funsuite.AnyFunSuite

class DeequCheckGroupTest extends AnyFunSuite {

  test("add expectation") {
    val deequCheckGroup = new DeequCheckGroup(DQGroup("test", "descr", "Warning", List()))
    deequCheckGroup.addExpectation(DQConstraint(List("c1"), "isUnique"))
    val actual = deequCheckGroup.check
    val expected = Check(CheckLevel.Warning, "descr").isUnique("c1")

    assert(actual.toString === expected.toString)
  }

  test("add a list of expectations") {
    val deequCheckGroup = new DeequCheckGroup(DQGroup("test", "descr", "Warning", List()))
    deequCheckGroup.addExpectations(List(DQConstraint(List("c1"), "isUnique"), DQConstraint(List("c1"), "isComplete")))
    val actual = deequCheckGroup.check
    val expected = Check(CheckLevel.Warning, "descr").isUnique("c1").isComplete("c1")

    assert(actual.toString === expected.toString)
  }

  test("convert a dq constraint into a deequ check") {
    val deequCheckGroup = new DeequCheckGroup(DQGroup("test", "descr", "Warning", List()))
    deequCheckGroup.convertConstraint(DQConstraint(List("c1"), "isUnique"))
    val actual = deequCheckGroup.check
    val expected = Check(CheckLevel.Warning, "descr").isUnique("c1")

    assert(actual.toString === expected.toString)
  }

  test("convert a dq constraint on more columns into a deequ check") {
    val deequCheckGroup = new DeequCheckGroup(DQGroup("test", "descr", "Warning", List()))
    deequCheckGroup.convertConstraint(DQConstraint(List("c1", "c2"), "isUnique"))
    val actual = deequCheckGroup.check
    val expected = Check(CheckLevel.Warning, "descr").isUnique("c1").isUnique("c2")

    assert(actual.toString === expected.toString)
  }

  // test all available checks
  val deequChecks: Map[DQConstraint, Check] = Map[DQConstraint, Check](
    DQConstraint(List("c1"), "isUnique") -> Check(CheckLevel.Warning, "descr").isUnique("c1"),
    DQConstraint(List("c1"), "isComplete") -> Check(CheckLevel.Warning, "descr").isComplete("c1"),
    DQConstraint(List("c1"), "hasCompleteness", Some(">="), Some("0.5")) -> Check(CheckLevel.Warning, "descr").hasCompleteness("c1", _ >= 0.5, Some("c1 should be above 0.5")),
    DQConstraint(List("c1"), "isContainedIn", value_list = Some(List("0", "1"))) -> Check(CheckLevel.Warning, "descr").isContainedIn("c1", Array("0", "1"), Some(s"Following condition should be true: 0 <= c1 <= 1")),
    DQConstraint(List("c1"), "isNonNegative") -> Check(CheckLevel.Warning, "descr").isNonNegative("c1"),
    DQConstraint(List("c1"), "hasUniqueness", Some(">="), Some("0.5")) -> Check(CheckLevel.Warning, "descr").hasUniqueness("c1", _ >= 0.5, Some("c1 should be above 0.5")),
  )
  deequChecks.foreach(
    c =>
      test(s"conversion ${c._1}") {
        val deequCheckGroup = new DeequCheckGroup(DQGroup("test", "descr", "Warning", List()))
        deequCheckGroup.convertConstraint(c._1)
        val actual = deequCheckGroup.check
        val expected = c._2

        assert(actual.toString === expected.toString)
      }
  )

}
