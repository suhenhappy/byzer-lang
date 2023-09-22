package tech.mlsql.job.listeners

import cn.hutool.core.date.DateUtil
import cn.hutool.db.{Db, Entity}
import streaming.core.strategy.platform.PlatformManager
import tech.mlsql.job.{JobListener, JobStatusListener}
/**
 * @Author : Wu.D.J
 * @Create : 2020.10.22
 */
class TaskIdentifyListener extends JobStatusListener {



    override def onJobRunning(event: JobListener.JobFinishedStatusEvent): Unit = {
        writeExecStatus(event)
    }

    override def onJobSuccess(event: JobListener.JobFinishedStatusEvent): Unit = {
        writeExecStatus(event)
/*        val writer = new FileWriter(path)
        writer.write("taskId:" + event.groupId + " - timestamp:" + System.currentTimeMillis() + " - status:"+ event.status +"\n")
        writer.flush()
        writer.close()*/
    }

    override def onJobError(event: JobListener.JobFinishedStatusEvent): Unit = {
        writeExecStatus(event)
/*        val writer = new FileWriter(path)
        writer.write("taskId:" + event.groupId + " - timestamp:" + System.currentTimeMillis() + " - status:"+ event.status +"\n")
        writer.flush()
        writer.close()*/
    }

    def writeExecStatus(event: JobListener.JobFinishedStatusEvent): Unit ={

        if("0".equals(transIdIsExits(event))){
            Db.use(PlatformManager.datasource).insert(
                Entity.create("t_etl_trans_scheme")
                  .set("TRANS_ID", event.transId)
                  .set("START_TIME",  DateUtil.format(DateUtil.date(),"yyyyMMddHHmmss"))
                  .set("END_TIME", DateUtil.format(DateUtil.date(),"yyyyMMddHHmmss"))
                  .set("EXEC_STATUS", event.status)
                  .set("CURR_TASK_ID", event.groupId)
            );
        }else{
            Db.use(PlatformManager.datasource).update(
                Entity.create().set("EXEC_STATUS", event.status)
                  .set("CURR_TASK_ID",event.groupId)
                  .set("START_TIME", DateUtil.format(DateUtil.date(),"yyyyMMddHHmmss"))
                  .set("END_TIME", DateUtil.format(DateUtil.date(),"yyyyMMddHHmmss")), //修改的数据
                Entity.create("t_etl_trans_scheme").set("TRANS_ID", event.transId) //where条件
            );
        }

    }

    def transIdIsExits(event: JobListener.JobFinishedStatusEvent): String ={
        return Db.use(PlatformManager.datasource).query("select count(1) from t_etl_trans_scheme where TRANS_ID=?", event.transId).get(0).get("count(1)").toString
    }


}
