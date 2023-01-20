package it.prima.datalake.quality.core.suite
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import it.prima.datalake.quality.core.check.{CheckGroup, DeequCheckGroup}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DeequSuite(target: DataFrame)(implicit spark: SparkSession)  extends Suite {

  private final val CHECK = "check"
  private final val CHECK_GROUP = "check_group"

  private[suite] val verificationSuite = VerificationSuite().onData(target)

  override def addCheckGroup(checkGroup: CheckGroup): Suite = {
    val deequCG = checkGroup.asInstanceOf[DeequCheckGroup]
    verificationSuite.addCheck(deequCG.check)
    this
  }

  override def runSuite(): DataFrame = {
    val verificationResult = verificationSuite.run()

    println(s"Verification ended with result: ${verificationResult.status}")
    VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
      .withColumnRenamed(CHECK, CHECK_GROUP)
  }

}
