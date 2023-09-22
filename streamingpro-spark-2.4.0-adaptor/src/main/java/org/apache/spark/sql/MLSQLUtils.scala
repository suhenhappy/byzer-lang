package org.apache.spark.sql

import java.lang.reflect.Type

import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.status.api.v1
import org.apache.spark.util.Utils
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer


object MLSQLUtils {
  def getJavaDataType(tpe: Type): (DataType, Boolean) = {
    JavaTypeInference.inferDataType(tpe)
  }

  def getContextOrSparkClassLoader(): ClassLoader = {
    Utils.getContextOrSparkClassLoader
  }

  def localCanonicalHostName = {
    Utils.localCanonicalHostName()
  }

  def getAppStatusStore(sparkSession: SparkSession) = {
    sparkSession.sparkContext.statusStore
  }

  def createStage(stageId: Int) = {
    new v1.StageData(
      v1.StageStatus.PENDING,
      stageId,
      0, 0, 0, 0, 0, 0, 0,
      0L, 0L, None, None, None, None,
      0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
      "Unknown",
      None,
      "Unknown",
      null,
      Nil,
      Nil,
      None,
      None,
      Map())
  }

  def createExplainCommand(lg: LogicalPlan, extended: Boolean) = {
    ExplainCommand(lg, extended = extended)
  }

  def createUserDefinedFunction(f: AnyRef,
                                dataType: DataType,
                                inputTypes: Option[Seq[DataType]]): UserDefinedFunction = {
    UserDefinedFunction(f, dataType, inputTypes)
  }

  def getConfiguration(sparkSession: SparkSession, key: String): String = {
    val conf = sparkSession.sparkContext.conf
    conf.get(key)
  }

  case class JobExecuteInfo(@BeanProperty groupId: String, @BeanProperty jobId: String,@BeanProperty status: String,@BeanProperty name:String,@BeanProperty description:String,@BeanProperty completionTime:String,@BeanProperty submissionTime:String)

  def getJobGroup(sparkSession: SparkSession, groupId: String) : String = {
    val store = getAppStatusStore(sparkSession)
    //    val jobList = Seq.empty[JobExecuteInfo]
    val jobList = ArrayBuffer[JobExecuteInfo]()
    for (job <- store.jobsList(null)) {
      if (!job.jobGroup.isEmpty) {
        val name = job.name // 任务名称
        val description = job.description.toString.replaceAll("Some\\(", "").replaceAll("\\)", "") // 任务描述
        val completionTime = job.completionTime.toString.replaceAll("Some\\(", "").replaceAll("\\)", "") // 完成时间
        val submissionTime = job.submissionTime.toString.replaceAll("Some\\(", "").replaceAll("\\)", "") // 提交时间
        val jobGroupId = job.jobGroup.get
        // job.jobGroup.isEmpty

        val jobId = job.jobId
        if (jobGroupId.equals(groupId)) {
          val jobInfo = JobExecuteInfo(jobGroupId, jobId.toString, job.status.name(), name.toString, description, CurrentHostIPUtils.getFormatDate(completionTime), CurrentHostIPUtils.getFormatDate(submissionTime))
          jobList += jobInfo
        }
      }
    }

    new Gson().toJson(jobList.toArray)
  }

}
