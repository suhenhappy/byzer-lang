package org.apache.spark.sql

import java.lang.reflect.Type

import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExplainMode, ExtendedMode}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedFunction}
import org.apache.spark.sql.types.DataType
import org.apache.spark.status.api.v1
import org.apache.spark.util.{CurrentHostIPUtils, Utils}

import scala.beans.BeanProperty
import com.google.gson.Gson

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


  def getConfiguration(sparkSession: SparkSession, key: String): String = {
    val conf = sparkSession.sparkContext.conf
    conf.get(key)
  }

  case class JobExecuteInfo(@BeanProperty groupId: String, @BeanProperty jobId: String, @BeanProperty status: String, @BeanProperty name: String, @BeanProperty description: String, @BeanProperty completionTime: String, @BeanProperty submissionTime: String)

  def getJobGroup(sparkSession: SparkSession, groupId: String): String = {
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

  def getJobTasks(sparkSession: SparkSession, groupId: String): String = {
    val store = getAppStatusStore(sparkSession)
    //    val jobList = Seq.empty[JobExecuteInfo]
    val jobList = ArrayBuffer[JobExecuteInfo]()
    val taskString = new StringBuilder
    for (job <- store.jobsList(null)) {
      if (!job.jobGroup.isEmpty) {
        val jobGroupId = job.jobGroup.get
        // job.jobGroup.isEmpty

        if (jobGroupId.equals(groupId)) {
          for (elem <- job.stageIds) {
            val stageData = store.stageData(elem)
            for (stage <- stageData) {
              val tasks = store.taskList(stage.stageId, stage.attemptId, Int.MaxValue)
              for (task <- tasks) {
                taskString.append(task.taskId)
                taskString.append(",")
              }
            }
          }
        }
      }
    }
    if(taskString.length>0){
      taskString.setLength(taskString.length - 1)
    }
    taskString.toString()
  }


  def createStage(stageId: Int) = {
    new v1.StageData(
      status = v1.StageStatus.PENDING,
      stageId = stageId,
      attemptId = 0,
      numTasks = 0,
      numActiveTasks = 0,
      numCompleteTasks = 0,
      numFailedTasks = 0,
      numKilledTasks = 0,
      numCompletedIndices = 0,

      submissionTime = None,
      firstTaskLaunchedTime = None,
      completionTime = None,
      failureReason = None,

      executorDeserializeTime = 0L,
      executorDeserializeCpuTime = 0L,
      executorRunTime = 0L,
      executorCpuTime = 0L,
      resultSize = 0L,
      jvmGcTime = 0L,
      resultSerializationTime = 0L,
      memoryBytesSpilled = 0L,
      diskBytesSpilled = 0L,
      peakExecutionMemory = 0L,
      inputBytes = 0L,
      inputRecords = 0L,
      outputBytes = 0L,
      outputRecords = 0L,
      shuffleRemoteBlocksFetched = 0L,
      shuffleLocalBlocksFetched = 0L,
      shuffleFetchWaitTime = 0L,
      shuffleRemoteBytesRead = 0L,
      shuffleRemoteBytesReadToDisk = 0L,
      shuffleLocalBytesRead = 0L,
      shuffleReadBytes = 0L,
      shuffleReadRecords = 0L,
      shuffleWriteBytes = 0L,
      shuffleWriteTime = 0L,
      shuffleWriteRecords = 0L,

      name = "Unknown",
      description = None,
      details = "Unknown",
      schedulingPool = null,

      rddIds = Nil,
      accumulatorUpdates = Nil,
      tasks = None,
      executorSummary = None,
      killedTasksSummary = Map(),
      resourceProfileId=0,
      peakExecutorMetrics=None
    )

  }

  def createExplainCommand(lg: LogicalPlan, extended: Boolean) = {
    ExplainCommand(lg, ExplainMode.fromString(ExtendedMode.name))
  }

  def createUserDefinedFunction(f: AnyRef,
                                dataType: DataType,
                                inputTypes: Option[Seq[DataType]]): UserDefinedFunction = {
    SparkUserDefinedFunction(f, dataType, Nil)
  }

}
