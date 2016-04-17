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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.sources.BaseRelation

/**
  * Created by alex on 03.04.16.
  *
  * Dummy Logical Relation used to avoid the planning of the holden child relation. the optimization of the holed
  * relation is supposed to be done separately as this relation doesn't garant the any
  * plannig ( Optimzer and Planner will just ignore it). The baserelation  parameter should
  * be used for mapping and later transformation on the plan ( by remplacing this node with
  * the planned version ). This classe  will produce a DataScanHolder sparkPlan with the same characteristics.
  *
  */
case class HolderLogicalRelation(child : LogicalPlan,  relation: BaseRelation ) extends UnaryNode{


  override def output: Seq[Attribute] = child.output
}
