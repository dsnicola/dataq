package it.prima.datalake.quality.core.abstract_factory

import com.amazon.deequ.checks.{Check, CheckLevel}
import it.prima.datalake.quality.core.analyzer.DeequAnalyzer
import it.prima.datalake.quality.core.check.DeequCheckGroup
import it.prima.datalake.quality.core.suite.DeequSuite
import it.prima.datalake.quality.model.{DQConstraint, DQGroup}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{a, convertToAnyMustWrapper}

class DeequFactoryTest extends AnyFunSuite {

  implicit val spark: SparkSession = null

  test("create deequ check group") {
    val deequFactory = new DeequFactory
    val actual = deequFactory.createCheckGroup(DQGroup("test", "descr", "Warning", List(DQConstraint(List("c1"), "isUnique"))))

    actual mustBe a[DeequCheckGroup]
  }

  test("create check group with wanted check") {
    val deequFactory = new DeequFactory
    val actual = deequFactory.createCheckGroup(DQGroup("test", "descr", "Warning", List(DQConstraint(List("c1"), "isUnique")))).asInstanceOf[DeequCheckGroup].check
    val expected = Check(CheckLevel.Warning, "descr").isUnique("c1")

    assert(actual.canEqual(expected))
  }

  test("create suite") {
    val deequFactory = new DeequFactory
    val actual = deequFactory.createSuite(null)

    actual mustBe a[DeequSuite]
  }

  test("create analyzer") {
    val deequFactory = new DeequFactory
    val actual = deequFactory.createAnalyzer

    actual mustBe a[DeequAnalyzer]
  }
}
