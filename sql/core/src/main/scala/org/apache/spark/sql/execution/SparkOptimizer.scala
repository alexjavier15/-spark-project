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

import org.apache.spark.sql.ExperimentalMethods
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode,LogicalPlan}
import org.apache.spark.sql.execution.{BinaryNode,UnaryNode,LeafNode}

import scala.collection.mutable
class SparkOptimizer(experimentalMethods: ExperimentalMethods) extends Optimizer {
  override def batches: Seq[Batch] = super.batches :+ Batch(
    "User Provided Optimizers", FixedPoint(100), experimentalMethods.extraOptimizations: _*)
}

object SparkOptimizer {


  val _optimizedPlans : mutable.Map[Int,SparkPlan] =  mutable.Map[Int, SparkPlan]()
  var _hits = 0
  var _adds = 0

  def addOptimzedPlan(simpleHash : Int , sparkPlan: SparkPlan): Unit = {

    if(!_optimizedPlans.contains(simpleHash)){
    _optimizedPlans+=(simpleHash-> sparkPlan)
      _adds+=1
    }

  }
  def getOptimizedPlan(simpleHash : Int): Option[SparkPlan] = {
   /* val res =_optimizedPlans.get(simpleHash)
    if(res.isDefined)
      _hits+=1
    res*/
    None

  }

  def clear():Unit={

    println("***Clearing SparkOptimzer Cache. adds: "+_adds+", hits: "+_hits+"*******")
    _optimizedPlans.clear()
    _adds=0
    _hits=0
  }


}
