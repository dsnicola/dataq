package it.prima.datalake.quality.core.suite

import it.prima.datalake.quality.core.check.{CheckGroup, DeequCheckGroup}
import it.prima.datalake.quality.model.{DQConstraint, DQGroup}
import it.prima.datalake.quality.utils.TestUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DeequSuiteTest extends AnyFunSuite {

  private implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


  test("add check group") {
    val suite = new DeequSuite(null)
    val deequCheckGroup = new DeequCheckGroup(DQGroup("test", "descr", "Warning", List()))
    suite.addCheckGroup(deequCheckGroup)

    assert(true)
  }

  test("add check group with no deequ instance") {
    val suite = new DeequSuite(null)
    val checkGroup = new CheckGroup {
      override def addExpectation(dqConstraint: DQConstraint): CheckGroup = ???
      override def addExpectations(dqConstraints: List[DQConstraint]): CheckGroup = ???
    }
    assertThrows[ClassCastException] {
      suite.addCheckGroup(checkGroup)
    }

  }

  test("run test suite") {
    val inputDataframe = TestUtils.getSampleDataFrame(spark)


    val suite = new DeequSuite(inputDataframe)
    val checks = List(
      DQConstraint(List("id"), "isUnique"),
      DQConstraint(List("age"), "isComplete")
    )
    val deequCheckGroup = new DeequCheckGroup(DQGroup("test", "descr", "Warning", checks)).addExpectations(checks)
    suite.addCheckGroup(deequCheckGroup)

    val actual = suite.runSuite()
    val expectedData = Seq(
      Row("descr", "Warning", "Warning", "UniquenessConstraint(Uniqueness(List(id),None))", "Success", ""),
      Row("descr", "Warning", "Warning", "CompletenessConstraint(Completeness(age,None))", "Failure", "Value: 0.75 does not meet the constraint requirement!")
    )
    val expectedSchema = StructType(List(
      StructField("check", StringType, true),
      StructField("check_level", StringType, true),
      StructField("check_status", StringType, true),
      StructField("constraint", StringType, true),
      StructField("constraint_status", StringType, true),
      StructField("constraint_message", StringType, true)
    ))
    val expected = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema
    )

    assert(actual.collectAsList() === expected.collectAsList())
  }
}
