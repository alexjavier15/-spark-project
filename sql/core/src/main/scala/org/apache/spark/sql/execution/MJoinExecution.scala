
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.{JobExecutionStatus, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.util.QueryExecutionListener

import scala.collection.mutable

/**
  * Created by arivas on 4/8/16.
  */
class MJoinExecution(plan: LogicalPlan,
                     baserels: Seq[LogicalPlan],
                     reorderedPlans: Seq[LogicalPlan],
                     chunkedPlans: Seq[LogicalPlan],
                     sqlContext: SQLContext


                    ) {


  private val activeExecutions = mutable.HashMap[Long, LogicalPlan]()
  private val pendingExecutions = mutable.HashMap[Long, LogicalPlan]()
  private val pendingExecutionsByJobId = mutable.HashMap[Long, Seq[Int]]()

  private val listeners = mutable.HashMap[Int, SparkListener]()
  private val jobsStatus = mutable.HashMap[Int, JobExecutionStatus]()
  private var relID: Int = 0
  private var nextPlans: Seq[LogicalPlan] = null
  private val sc = sqlContext.sparkContext

  def nextRelID(): Int = {
    relID += 1
    relID
  }



  def onSuccesJob(jobId: Int): Unit = {

    jobsStatus(jobId) = JobExecutionStatus.SUCCEEDED
    if(hasNextPlan)
      executeNext()

  }

  def hasNextPlan: Boolean = {
    val next = pendingExecutionsByJobId.filter(entry => {

      !entry._2.exists(jobsStatus(_) != JobExecutionStatus.SUCCEEDED)
    }).keys.toSeq

    if (next.nonEmpty) {
      nextPlans = pendingExecutions.filter(entry => next.contains(entry._1)).values.toSeq
      return true

    }
    false
  }

  def executeNext() : Unit ={
    val queryExecution = nextPlans.map( new QueryExecution(sqlContext,_))
  //  nextPlans.foreach(pendingExecutions.remove(_))

  }

}

private[sql] class TestMJoinListener()  extends SparkListener with Logging {

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

    println("***************JOB ID**********"+ jobEnd.jobId+"**********")

    }

}
private[sql] class MJoinListener(conf: SparkConf,
                                 mjoinExecution: MJoinExecution
                                ) extends SparkListener with Logging {
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

    mjoinExecution.onSuccesJob(jobEnd.jobId)

  }


}
