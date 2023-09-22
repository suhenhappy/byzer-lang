package tech.mlsql.job

import tech.mlsql.job.JobListener.JobFinishedStatusEvent

abstract class JobListener {

  import JobListener._

  def onJobStarted(event: JobStartedEvent): Unit

  def onJobFinished(event: JobFinishedEvent): Unit

}

abstract class JobStatusListener {

  import JobListener._

  def onJobRunning(event: JobFinishedStatusEvent): Unit

  def onJobSuccess(event: JobFinishedStatusEvent): Unit

  def onJobError(event: JobFinishedStatusEvent): Unit
}

object JobListener {

  trait JobEvent

  class JobStartedEvent(val groupId:String) extends JobEvent

  class JobFinishedEvent(val groupId:String) extends JobEvent

  class JobFinishedStatusEvent(val groupId:String,val transId:String, val status:String) extends JobEvent

}
